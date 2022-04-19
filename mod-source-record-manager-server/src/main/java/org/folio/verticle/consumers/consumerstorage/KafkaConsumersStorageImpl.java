package org.folio.verticle.consumers.consumerstorage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.KafkaConsumerWrapper;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class KafkaConsumersStorageImpl implements KafkaConsumersStorage {
  private static final Logger LOGGER = LogManager.getLogger();

  private final Map<String, List<KafkaConsumerWrapper<String, String>>> consumerWrappersMap = Collections.synchronizedMap(new HashMap<>());

  @Override
  public void addConsumer(String eventName, KafkaConsumerWrapper<String, String> consumer) {
    consumerWrappersMap.computeIfAbsent(eventName, k -> new ArrayList<>()).add(consumer);
  }

  @Override
  public List<KafkaConsumerWrapper<String, String>> getConsumersByEvent(String eventName) {
    return consumerWrappersMap.get(eventName);
  }

  @Override
  public List<KafkaConsumerWrapper<String, String>> getConsumersList() {
    return consumerWrappersMap.values()
      .stream()
      .flatMap(Collection::stream)
      .collect(Collectors.toList());
  }
}
