package org.folio.verticle.consumers.consumerstorage;

import org.folio.kafka.KafkaConsumerWrapper;

import java.util.Collection;

/**
 * Storage for Kafka consumers. Consumers are not tenant specific.
 */
public interface KafkaConsumersStorage {

  /**
   * Adds new consumer.
   * @param eventName the event name that is the key of consumer
   * @param consumer  consumer wrapper to add
   */
  void addConsumer(String eventName, KafkaConsumerWrapper<?,?> consumer);

  /**
   * Gets consumer by event name.
   *
   * @param eventName the event name
   * @return consumer wrappers by event name
   */
  Collection<KafkaConsumerWrapper<?,?>> getConsumersByEvent(String eventName);

  /**
   * Gets all registered consumers.
   *
   * @return collection of all registered consumer wrappers
   */
  Collection<KafkaConsumerWrapper<?,?>> getConsumersList();
}
