package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class QuickMarcEventProducerServiceImpl implements QuickMarcEventProducerService {

  private static final Logger LOGGER = LogManager.getLogger();
  @Value("${ENV:folio}")
  private String envId;
  @Autowired
  private KafkaProducerService kafkaProducerService;

  @Override
  public Future<Boolean> sendEvent(String eventPayload, String eventType, String key, String tenantId,
                                   List<KafkaHeader> kafkaHeaders) {
    return sendEventInternal(eventPayload, eventType, key, tenantId, kafkaHeaders, false);
  }

  @Override
  public Future<Boolean> sendEventWithZipping(String eventPayload, String eventType, String key, String tenantId,
                                              List<KafkaHeader> kafkaHeaders) {
    return sendEventInternal(eventPayload, eventType, key, tenantId, kafkaHeaders, true);
  }

  private Future<Boolean> sendEventInternal(String eventPayload, String eventType, String key, String tenantId,
                                            List<KafkaHeader> kafkaHeaders, boolean isZipped) {
    Promise<Boolean> promise = Promise.promise();
    try {
      var event = kafkaProducerService.createEvent(eventPayload, eventType, tenantId, isZipped);
      var topicName = kafkaProducerService.createTopicName(eventType, tenantId, envId);
      var record = kafkaProducerService.createProducerRecord(event, key, topicName, kafkaHeaders);
      kafkaProducerService.sendEvent(eventType, record);
    } catch (Exception e) {
      promise.fail(e);
    }
    return promise.future();
  }
}
