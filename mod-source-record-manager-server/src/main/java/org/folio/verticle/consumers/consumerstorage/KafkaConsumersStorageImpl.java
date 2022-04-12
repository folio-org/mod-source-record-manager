package org.folio.verticle.consumers.consumerstorage;

import org.folio.kafka.KafkaConsumerWrapper;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

@Component
public class KafkaConsumersStorageImpl implements KafkaConsumersStorage {
  private Map<String, KafkaConsumerWrapper<String, String>> consumerWrappers = new HashMap<>();

  @Override
  public void addConsumer(String eventName, KafkaConsumerWrapper<String, String> consumerWrapper) {
    consumerWrappers.put(eventName, consumerWrapper);
  }

  @Override
  public KafkaConsumerWrapper<String, String> getConsumer(String eventName) {
    return consumerWrappers.get(eventName);
  }

  @Override
  public Collection<KafkaConsumerWrapper<String, String>> getConsumersList() {
    return consumerWrappers.values();
  }
}
