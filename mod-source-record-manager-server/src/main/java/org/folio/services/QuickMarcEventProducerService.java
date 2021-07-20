package org.folio.services;

import java.util.List;

import io.vertx.core.Future;
import io.vertx.kafka.client.producer.KafkaHeader;

/**
 * Service for processing quick marc events
 */
public interface QuickMarcEventProducerService {

  /**
   * Publishes an event with each of the passed records to the specified topic
   *
   * @param eventPayload payload
   * @param eventType    event type
   * @param key          key with which the specified event
   * @param tenantId     tenant id
   * @param kafkaHeaders kafka headers
   * @return true if successful
   */
  Future<Boolean> sendEvent(String eventPayload, String eventType, String key, String tenantId,
                            List<KafkaHeader> kafkaHeaders);
}
