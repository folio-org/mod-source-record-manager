package org.folio.services.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.util.pubsub.PubSubClientUtils;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

public final class EventHandlingUtil {

  private EventHandlingUtil() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(EventHandlingUtil.class);

  /**
   * Prepares and sends event with zipped payload to the mod-pubsub
   *
   * @param eventPayload eventPayload in String representation
   * @param eventType    eventType
   * @param params       connection parameters
   * @return completed future with true if event was sent successfully
   */
  public static Future<Boolean> sendEventWithPayloadToPubSub(String eventPayload, String eventType, OkapiConnectionParams params) {
    Promise<Boolean> promise = Promise.promise();
    try {
      Event event = new Event()
        .withId(UUID.randomUUID().toString())
        .withEventType(eventType)
        .withEventPayload(ZIPArchiver.zip(eventPayload))
        .withEventMetadata(new EventMetadata()
          .withTenantId(params.getTenantId())
          .withEventTTL(1)
          .withPublishedBy(PubSubClientUtils.constructModuleName()));

      org.folio.rest.util.OkapiConnectionParams connectionParams = new org.folio.rest.util.OkapiConnectionParams();
      connectionParams.setOkapiUrl(params.getOkapiUrl());
      connectionParams.setToken(params.getToken());
      connectionParams.setTenantId(params.getTenantId());
      connectionParams.setVertx(params.getVertx());

      PubSubClientUtils.sendEventMessage(event, connectionParams)
        .whenComplete((ar, throwable) -> {
          if (throwable == null) {
            promise.complete(true);
          } else {
            LOGGER.error("Error during event sending: {}", throwable, event);
            promise.fail(throwable);
          }
        });
    } catch (Exception e) {
      LOGGER.error("Failed to send {} event to mod-pubsub", e, eventType);
      promise.fail(e);
    }
    return promise.future();
  }

  /**
   * Prepares and sends event with zipped payload to kafka
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
    Event event;
    try {
      event = new Event()
        .withId(UUID.randomUUID().toString())
        .withEventType(eventType)
        .withEventPayload(ZIPArchiver.zip(eventPayload))
        .withEventMetadata(new EventMetadata()
          .withTenantId(tenantId)
          .withEventTTL(1)
          .withPublishedBy(PubSubClientUtils.constructModuleName()));
    } catch (IOException e) {
      LOGGER.error("Failed to construct an event for eventType {}", e, eventType);
      return Future.failedFuture(e);
    }

    String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), KafkaTopicNameHelper.getDefaultNameSpace(),
      tenantId, eventType);

    KafkaProducerRecord<String, String> record = KafkaProducerRecord.create(topicName, key, Json.encode(event));
    record.addHeaders(kafkaHeaders);

    Promise<Boolean> promise = Promise.promise();

    String producerName = eventType + "_Producer";
    KafkaProducer<String, String> producer =
      KafkaProducer.createShared(Vertx.currentContext().owner(), producerName, kafkaConfig.getProducerProps());

    producer.write(record, war -> {
      producer.end(ear -> producer.close());
      if (war.succeeded()) {
        LOGGER.info("Event with type {} was sent to kafka", eventType);
        promise.complete(true);
      } else {
        Throwable cause = war.cause();
        LOGGER.error("{} write error for event {}:", cause, producerName, eventType);
        promise.fail(cause);
      }
    });
    return promise.future();
  }

}
