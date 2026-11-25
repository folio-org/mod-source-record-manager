package org.folio.services;

import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * Produces round-robin Kafka message keys so that events are spread across topic partitions.
 */
@Component
public class DistributionKeyGenerator {

  private final AtomicInteger counter = new AtomicInteger();
  private final int maxDistributionNum;

  public DistributionKeyGenerator(
    @Value("${srm.kafka.RawChunksKafkaHandler.maxDistributionNum:100}") int maxDistributionNum) {
    this.maxDistributionNum = maxDistributionNum;
  }

  public String next() {
    return String.valueOf(counter.incrementAndGet() % maxDistributionNum);
  }
}
