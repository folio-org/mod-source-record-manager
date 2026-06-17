package org.folio;

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
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.Event;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import io.vertx.core.json.Json;

import java.time.Duration;
import java.util.ArrayList;
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
    = DockerImageName.parse("apache/kafka-native:4.2.0");

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
    List<ConsumerRecord<String, String>> records = new ArrayList<>();
    long timeoutNanos = timeUnit.toNanos(timeout);
    long deadlineNanos = System.nanoTime() + timeoutNanos;

    try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties)) {
      kafkaConsumer.subscribe(Collections.singletonList(topicToObserve));

      while (records.size() < amountOfEvents && System.nanoTime() < deadlineNanos) {
        long remainingNanos = Math.max(0L, deadlineNanos - System.nanoTime());
        long pollTimeoutMs = Math.max(100L, Math.min(1000L, TimeUnit.NANOSECONDS.toMillis(remainingNanos)));
        ConsumerRecords<String, String> polled = kafkaConsumer.poll(Duration.ofMillis(pollTimeoutMs));
        polled.forEach(records::add);
      }

      assert records.size() == amountOfEvents :
        String.format("Expected %d events, but found %d", amountOfEvents, records.size());
    }

    return records;
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

  public static List<DataImportEventPayload> waitForEventsByJobExecutionId(String topic,
                                                                            String jobExecutionId,
                                                                            int expectedCount,
                                                                            long timeoutSeconds) {
    long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
    List<DataImportEventPayload> matchedPayloads = new ArrayList<>();

    Properties consumerProperties = new Properties();
    consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
    consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + java.util.UUID.randomUUID());
    consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    try (KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(consumerProperties)) {
      kafkaConsumer.subscribe(Collections.singletonList(topic));

      while (matchedPayloads.size() < expectedCount && System.nanoTime() < deadlineNanos) {
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(500));
        records.forEach(consumerRecord -> {
          Event obtainedEvent = Json.decodeValue(consumerRecord.value(), Event.class);
          DataImportEventPayload eventPayload = Json.decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);
          if (jobExecutionId.equals(eventPayload.getJobExecutionId())) {
            matchedPayloads.add(eventPayload);
          }
        });
      }
    }

    return matchedPayloads;
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
