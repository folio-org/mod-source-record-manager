package org.folio.services.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.utils.PomReaderUtil;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.tools.utils.ModuleName;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public final class EventHandlingUtil {

  private EventHandlingUtil() {
  }

  private static final Logger LOGGER = LogManager.getLogger();

  /**
   * Prepares and sends event with payload to kafka
   *
   * @param tenantId     tenant id
   * @param eventPayload eventPayload in String representation
   * @param eventType    eventType
   * @param kafkaHeaders kafka headers
   * @param kafkaConfig  kafka config
   * @return completed future with true if event was sent successfully
   */
  public static Future<Boolean> sendEventToKafka(String tenantId, String eventPayload, String eventType,
                                                 List<KafkaHeader> kafkaHeaders, KafkaConfig kafkaConfig, String key) {
    LOGGER.debug("Starting to send event to Kafka for eventType: {}", eventType);
    Event event;
    try {
      event = createEvent(eventPayload, eventType, tenantId, false);
    } catch (IOException e) {
      LOGGER.error("Failed to construct an event for eventType {}", eventType, e);
      return Future.failedFuture(e);
    }

    String topicName = createTopicName(eventType, tenantId, kafkaConfig);

    KafkaProducerRecord<String, String> record = createProducerRecord(event, key, topicName, kafkaHeaders);

    Promise<Boolean> promise = Promise.promise();

    String correlationId = extractHeader(kafkaHeaders, "correlationId");
    String recordId = extractHeader(kafkaHeaders, "recordId");

    String producerName = eventType + "_Producer";
    KafkaProducer<String, String> producer =
      KafkaProducer.createShared(Vertx.currentContext().owner(), producerName, kafkaConfig.getProducerProps());
    producer.write(record, war -> {
      producer.end(ear -> producer.close());
      if (war.succeeded()) {
        logSendingSucceeded(eventType, correlationId, recordId);
        promise.complete(true);
      } else {
        Throwable cause = war.cause();
        LOGGER.error("{} write error for event {}:", producerName, eventType, cause);
        promise.fail(cause);
      }
    });
    return promise.future();
  }

  private static void logSendingSucceeded(String eventType, String correlationId, String recordId) {
    if (recordId == null) {
      LOGGER.info("Event with type: {} and correlationId: {} was sent to kafka", eventType, correlationId);
    } else {
      LOGGER.info("Event with type: {} and recordId: {} was sent to kafka", eventType, recordId);
    }
  }

  private static String extractHeader(List<KafkaHeader> kafkaHeaders, String headerName) {
    return kafkaHeaders.stream()
      .filter(header -> header.key().equals(headerName))
      .findFirst()
      .map(header -> header.value().toString())
      .orElse(null);
  }

  public static KafkaProducerRecord<String, String> createProducerRecord(Event event, String key, String topicName, List<KafkaHeader> kafkaHeaders) {
    KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(topicName, key, Json.encode(event));
    record.addHeaders(kafkaHeaders);
    return record;
  }

  public static String createTopicName(String eventType, String tenantId, KafkaConfig kafkaConfig) {
    return KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), KafkaTopicNameHelper.getDefaultNameSpace(),
      tenantId, eventType);
  }

  public static Event createEvent(String eventPayload, String eventType, String tenantId, boolean isZipped) throws IOException {
    return new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(eventType)
      .withEventPayload(isZipped ? ZIPArchiver.zip(eventPayload) : eventPayload)
      .withEventMetadata(new EventMetadata()
        .withTenantId(tenantId)
        .withEventTTL(1)
        .withPublishedBy(constructModuleName()));
  }

  public static String constructModuleName() {
    return PomReaderUtil.INSTANCE.constructModuleVersionAndVersion(ModuleName.getModuleName(),
      ModuleName.getModuleVersion());
  }

  public static KafkaProducer<String, String> createProducer(String eventType, KafkaConfig kafkaConfig) {
    String producerName = eventType + "_Producer";
    return KafkaProducer.createShared(Vertx.currentContext().owner(), producerName, kafkaConfig.getProducerProps());
  }
}
