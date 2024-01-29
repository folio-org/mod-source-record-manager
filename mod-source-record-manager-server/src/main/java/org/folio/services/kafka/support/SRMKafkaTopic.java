package org.folio.services.kafka.support;

import org.folio.kafka.services.KafkaTopic;

public class SRMKafkaTopic implements KafkaTopic {

  private final String topic;
  private final int numPartitions;

  public SRMKafkaTopic(String topic, int numPartitions) {
    this.topic = topic;
    this.numPartitions = numPartitions;
  }

  @Override
  public String moduleName() {
    return "srm";
  }

  @Override
  public String topicName() {
    return topic;
  }

  @Override
  public int numPartitions() {
    return numPartitions;
  }
}
