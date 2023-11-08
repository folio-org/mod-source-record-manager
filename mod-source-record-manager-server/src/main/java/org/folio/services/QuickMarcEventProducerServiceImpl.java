package org.folio.services;

import static org.folio.services.util.EventHandlingUtil.createEvent;
import static org.folio.services.util.EventHandlingUtil.createProducer;
import static org.folio.services.util.EventHandlingUtil.createProducerRecord;
import static org.folio.services.util.EventHandlingUtil.createTopicName;
import static org.folio.verticle.consumers.util.QMEventTypes.QM_COMPLETED;
import static org.folio.verticle.consumers.util.QMEventTypes.QM_RECORD_UPDATED;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;

import org.folio.kafka.KafkaConfig;

@Service
public class QuickMarcEventProducerServiceImpl implements QuickMarcEventProducerService {

  private static final Logger LOGGER = LogManager.getLogger();
  private final KafkaConfig kafkaConfig;
  private final Map<String, KafkaProducer<String, String>> kafkaProducers = new HashMap<>();

  public QuickMarcEventProducerServiceImpl(KafkaConfig kafkaConfig) {
    this.kafkaConfig = kafkaConfig;
    kafkaProducers.put(QM_RECORD_UPDATED.name(), createProducer(QM_RECORD_UPDATED.name(), kafkaConfig));
    kafkaProducers.put(QM_COMPLETED.name(), createProducer(QM_COMPLETED.name(), kafkaConfig));
  }

  @Override
  public Future<Boolean> sendEvent(String eventPayload, String eventType, String key, String tenantId,
                                   List<KafkaHeader> kafkaHeaders) {
    return sendEventInternal(eventPayload, eventType, key, tenantId, kafkaHeaders);
  }

  @Override
  public Future<Boolean> sendEventWithZipping(String eventPayload, String eventType, String key, String tenantId,
                                              List<KafkaHeader> kafkaHeaders) {
    return sendEventInternal(eventPayload, eventType, key, tenantId, kafkaHeaders);
  }

  private Future<Boolean> sendEventInternal(String eventPayload, String eventType, String key, String tenantId,
                                            List<KafkaHeader> kafkaHeaders) {
    Promise<Boolean> promise = Promise.promise();
    try {
      var event = createEvent(eventPayload, eventType, tenantId);
      var topicName = createTopicName(eventType, tenantId, kafkaConfig);
      var record = createProducerRecord(event, key, topicName, kafkaHeaders);
      var producer = kafkaProducers.get(eventType);
      if (producer != null) {
        producer.write(record)
          .onSuccess(unused -> {
            LOGGER.info("sendEventInternal:: Event with type {} was sent to kafka", eventType);
            promise.complete(true);
          })
          .onFailure(throwable -> {
            var cause = throwable.getCause();
            LOGGER.warn("sendEventInternal:: Error while send event {}: {}", eventType, cause);
            promise.fail(cause);
          });
      } else {
        promise.fail("No producer found for event: " + eventType);
      }
    } catch (Exception e) {
      LOGGER.warn("sendEventInternal:: error while sending event", e);
      promise.fail(e);
    }
    return promise.future();
  }
}
