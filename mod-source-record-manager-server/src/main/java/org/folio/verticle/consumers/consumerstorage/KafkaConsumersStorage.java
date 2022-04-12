package org.folio.verticle.consumers.consumerstorage;

import org.folio.kafka.KafkaConsumerWrapper;

import java.util.Collection;

/**
 * Storage for Kafka consumers. Consumers are not tenant specific.
 */
public interface KafkaConsumersStorage {

  /**
   * Adds new consumer.
   *
   * @param eventName the event name that is the key of consumer
   * @param consumerWrapper newly added return consumer wrapper
   */
  void addConsumer(String eventName, KafkaConsumerWrapper<String, String> consumerWrapper);

  /**
   * Gets consumer by event name.
   *
   * @param eventName the event name
   * @return consumer wrapper by event name
   */
  KafkaConsumerWrapper<String, String> getConsumer(String eventName);

  /**
   * Gets all registered consumers.
   *
   * @return collection of all registered consumer wrappers
   */
  Collection<KafkaConsumerWrapper<String, String>> getConsumersList();
}
