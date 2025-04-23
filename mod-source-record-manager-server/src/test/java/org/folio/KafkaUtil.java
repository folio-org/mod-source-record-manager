package org.folio;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static java.time.Duration.ofMinutes;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

public final class KafkaUtil {
  private static final Logger logger = LogManager.getLogger();

  public static final DockerImageName IMAGE_NAME
    = DockerImageName.parse("apache/kafka-native:3.8.0");

  private static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer(IMAGE_NAME)
    .withStartupAttempts(3);

  private KafkaUtil() {
    throw new UnsupportedOperationException("Cannot instantiate utility class.");
  }

  public static void startKafka() {
    KAFKA_CONTAINER.start();

    logger.info("starting Kafka host={} port={}",
      KAFKA_CONTAINER.getHost(), KAFKA_CONTAINER.getFirstMappedPort());

    var kafkaHost = KAFKA_CONTAINER.getHost();
    var kafkaPort = String.valueOf(KAFKA_CONTAINER.getFirstMappedPort());
    logger.info("Starting Kafka host={} port={}", kafkaHost, kafkaPort);
    System.setProperty("kafka-port", kafkaPort);
    System.setProperty("kafka-host", kafkaHost);

    await().atMost(ofMinutes(1)).until(KAFKA_CONTAINER::isRunning);

    logger.info("finished starting Kafka");
  }

  public static void stopKafka() {
    if (KAFKA_CONTAINER.isRunning()) {
      logger.info("stopping Kafka host={} port={}",
        KAFKA_CONTAINER.getHost(), KAFKA_CONTAINER.getFirstMappedPort());

      KAFKA_CONTAINER.stop();
      logger.info("finished stopping Kafka");
    } else {
      logger.info("Kafka container already stopped");
    }
  }

  public static List<ConsumerRecord<String, String>> checkKafkaEventSent(String topicToObserve, int amountOfEvents) {
    return checkKafkaEventSent(topicToObserve, amountOfEvents,3, TimeUnit.SECONDS);
  }

  public static List<ConsumerRecord<String, String>> checkKafkaEventSent(String topicToObserve, int amountOfEvents,
                                                                         long timeout, TimeUnit timeUnit) {
    Properties consumerProperties = getConsumerProperties();
    ConsumerRecords<String, String> records;

    try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties)) {
      kafkaConsumer.seekToBeginning(kafkaConsumer.assignment());

      kafkaConsumer.subscribe(Collections.singletonList(topicToObserve));
      records = kafkaConsumer.poll(Duration.of(timeout, timeUnit.toChronoUnit()));

      assert records.count() == amountOfEvents :
        String.format("Expected %d events, but found %d", amountOfEvents, records.count());
    }

    return Lists.newArrayList(records.iterator());
  }

  public static RecordMetadata sendEvent(ProducerRecord<String, String> producerRecord) throws ExecutionException, InterruptedException {
    var producerProperties = getProducerProperties();
    try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties)) {
      return kafkaProducer.send(producerRecord).get();
    }
  }

  public static void clearAllTopics() {
    Properties consumerProperties = getConsumerProperties();
    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
      Set<TopicPartition> partitions = consumer.listTopics().values().stream()
        .flatMap(partitionInfos -> partitionInfos.stream()
          .map(partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition())))
        .collect(java.util.stream.Collectors.toSet());

      if (!partitions.isEmpty()) {
        consumer.assign(partitions);
        consumer.seekToEnd(partitions);
        Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = partitions.stream()
          .collect(java.util.stream.Collectors.toMap(
            partition -> partition,
            partition -> new OffsetAndMetadata(consumer.position(partition))
          ));
        consumer.commitSync(offsetsToCommit);
      }
    }
  }

  public static String[] getKafkaHostAndPort() {
    return KAFKA_CONTAINER.getBootstrapServers().split(":");
  }

  public static List<String> getValues(List<ConsumerRecord<String, String>> consumerRecords) {
    return consumerRecords.stream()
      .map(ConsumerRecord::value)
      .toList();
  }

  private static Properties getConsumerProperties() {
    Properties consumerProperties = new Properties();
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return consumerProperties;
  }

  private static Properties getProducerProperties() {
    Properties producerProperties = new Properties();
    producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
    producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    return producerProperties;
  }
}
