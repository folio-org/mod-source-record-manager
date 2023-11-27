package org.folio.services.util;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.SimpleKafkaProducerManager;
import org.folio.kafka.services.KafkaProducerRecordBuilder;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.tools.utils.ModuleName;
import org.folio.services.exceptions.RecordsPublishingException;

import static org.folio.services.util.RecordConversionUtil.RECORDS;

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
    Event event = createEvent(eventPayload, eventType, tenantId);

    String topicName = createTopicName(eventType, tenantId, kafkaConfig);

    KafkaProducerRecord<String, String> record = createProducerRecord(event, key, topicName, kafkaHeaders);

    String chunkId = extractHeader(kafkaHeaders, "chunkId");
    String recordId = extractHeader(kafkaHeaders, "recordId");

    String producerName = eventType + "_Producer";
    LOGGER.debug("sendEventToKafka:: Starting to send event to Kafka for eventType: {} and recordId: {} and chunkId: {}", eventType, recordId, chunkId);

    KafkaProducer<String, String> producer = createProducer(eventType, kafkaConfig);
    return producer.send(record)
        .eventually(x -> producer.close())
        .map(true)
        .onSuccess(x -> logSendingSucceeded(eventType, chunkId, recordId))
        .recover(err -> handleKafkaPublishingErrors(eventPayload, producerName, eventType, err));
  }

  private static void logSendingSucceeded(String eventType, String chunkId, String recordId) {
    if (recordId == null) {
      LOGGER.info("logSendingSucceeded:: Event with type: {} and chunkId: {} was sent to kafka", eventType, chunkId);
    } else {
      LOGGER.info("logSendingSucceeded:: Event with type: {} and recordId: {} was sent to kafka", eventType, recordId);
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
    KafkaProducerRecord<String, String> record = new KafkaProducerRecordBuilder<String, Object>(event.getEventMetadata().getTenantId())
      .key(key)
      .value(event)
      .topic(topicName)
      .build();

    record.addHeaders(kafkaHeaders);
    return record;
  }

  public static String createTopicName(String eventType, String tenantId, KafkaConfig kafkaConfig) {
    return KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), KafkaTopicNameHelper.getDefaultNameSpace(),
      tenantId, eventType);
  }

  public static Event createEvent(String eventPayload, String eventType, String tenantId) {
    return new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(eventType)
      .withEventPayload(eventPayload)
      .withEventMetadata(new EventMetadata()
        .withTenantId(tenantId)
        .withEventTTL(1)
        .withPublishedBy(constructModuleName()));
  }

  public static String constructModuleName() {
    // PomReaderUtil.INSTANCE fails in unit tests: "IOException: Can't read module name - Model is empty!"
    return ModuleName.getModuleName().replace("_", "-") + "-" + ModuleName.getModuleVersion();
  }

  public static KafkaProducer<String, String> createProducer(String eventType, KafkaConfig kafkaConfig) {
    return new SimpleKafkaProducerManager(Vertx.currentContext().owner(), kafkaConfig).createShared(eventType);
  }

  static <T> Future<T> handleKafkaPublishingErrors(
      String eventPayload, String producerName, String eventType, Throwable cause) {

    var err = wrapKafkaException(eventPayload, cause);
    LOGGER.warn("{} write error for event {}:", producerName, eventType, err);
    return Future.failedFuture(err);
  }

  private static Throwable wrapKafkaException(String eventPayload, Throwable cause) {
    if (! new JsonObject(eventPayload).containsKey(RECORDS)) {
      return cause;
    }
    RecordCollection recordCollection = Json.decodeValue(eventPayload, RecordCollection.class);
    return new RecordsPublishingException(cause.getMessage(), recordCollection.getRecords());
  }
}
