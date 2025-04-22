package org.folio;

import com.google.common.collect.Lists;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

  public static List<ConsumerRecord<String, String>> checkKafkaEventSent(String eventType, int amountOfEvents) throws InterruptedException {
    return checkKafkaEventSent(eventType, amountOfEvents,3, TimeUnit.SECONDS);
  }

  /**
   * Checks if the specified number of events have been sent to a Kafka topic within a given timeout.
   *
   * @param topicToObserve The Kafka topic to observe for events.
   * @param amountOfEvents The number of events expected to be found in the topic.
   * @param timeout        The maximum time to wait for the events.
   * @param timeUnit       The unit of time for the timeout (e.g., seconds, milliseconds).
   * @return A list of ConsumerRecord objects containing the events retrieved from the topic.
   * @throws InterruptedException If the expected number of events are not found within the given timeout.
   */
  public static List<ConsumerRecord<String, String>> checkKafkaEventSent(String topicToObserve, int amountOfEvents,
                                                                         long timeout, TimeUnit timeUnit) throws InterruptedException {
    Properties consumerProperties = getConsumerProperties();
    List<ConsumerRecord<String, String>> allRecords = Lists.newArrayList();

    try (var kafkaConsumer = new KafkaConsumer<String, String>(consumerProperties)) {
      kafkaConsumer.subscribe(Collections.singletonList(topicToObserve));

      while (allRecords.size() < amountOfEvents) {
        var records = kafkaConsumer.poll(Duration.of(timeout, timeUnit.toChronoUnit()));
        logger.info("Found {} records in the topic {}", records.count(), topicToObserve);
        if (records.isEmpty()) {
          throw new InterruptedException("%s records not found in the given time".formatted(amountOfEvents - allRecords.size()));
        }
        records.forEach(allRecords::add);
      }
    }
    return allRecords;
  }

  public static RecordMetadata sendEvent(Map<String, String> kafkaHeaders,
                                         String topic, String key, String value) throws ExecutionException, InterruptedException {
    var producerProperties = getProducerProperties();
    try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(producerProperties)) {
      var producerRecord = new ProducerRecord<>(topic, key, value);
      kafkaHeaders.forEach((k, v) -> {
        if (v != null) {
          producerRecord.headers().add(k, v.getBytes());
        }
      });

      return kafkaProducer.send(producerRecord).get();
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
