package org.folio.services.util;

import io.vertx.core.Future;

import java.util.List;

/**
 * Service for creating Kafka topics
 */
public interface KafkaTopicService {

  /**
   * Creates Kafka topics for specified event types
   *
   * @param eventTypes list of event types, for which topics should be created
   * @param tenantId   tenant id, for which topics should be created
   * @return future with true if succeeded
   */
  Future<Boolean> createTopics(List<String> eventTypes, String tenantId);
}
