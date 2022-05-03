package org.folio.services.flowcontrol;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.verticle.consumers.consumerstorage.KafkaConsumersStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_READ;

@Service
public class RawRecordsFlowControlServiceImpl implements RawRecordsFlowControlService {
  private static final Logger LOGGER = LogManager.getLogger();

  @Value("${di.flow.max_simultaneous_records:20}")
  private Integer maxSimultaneousRecords;
  @Value("${di.flow.records_threshold:10}")
  private Integer recordsThreshold;
  @Value("${di.flow.control.enable:true}")
  private boolean enableFlowControl;

  @Autowired
  private KafkaConsumersStorage consumersStorage;

  private final AtomicInteger atomicCurrent = new AtomicInteger(0);

  @PostConstruct
  public void init() {
    LOGGER.info("Flow control feature is {}", enableFlowControl ? "enabled" : "disabled");
  }

  @Override
  public void trackChunkReceivedEvent(Integer initialRecordsCount) {
    if (!enableFlowControl) {
      return;
    }

    int current = atomicCurrent.addAndGet(initialRecordsCount);

    LOGGER.info("--------------- Current value after chunk received: {} ---------------", current);

    if (current >= maxSimultaneousRecords) {
      List<KafkaConsumerWrapper<String, String>> rawRecordsReadConsumers = consumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());

      rawRecordsReadConsumers.forEach(consumer -> {
        if (consumer.demand() > 0) {
          consumer.pause();

          LOGGER.info("Kafka consumer - id: {}, subscription - {} is paused, because {} exceeded {} max simultaneous records",
            consumer.getId(), DI_RAW_RECORDS_CHUNK_READ.value(), this.atomicCurrent, maxSimultaneousRecords);
        }
      });
    }
  }

  @Override
  public void trackChunkDuplicateEvent(Integer duplicatedRecordsCount) {
    int prev = atomicCurrent.get();

    atomicCurrent.set(prev - duplicatedRecordsCount);

    LOGGER.info("--------------- Chunk duplicate event comes, update current value from: {} to: {} ---------------", prev, atomicCurrent.get());

    resumeIfThresholdAllows(atomicCurrent.get());
  }

  @Override
  public void trackRecordCompleteEvent() {
    if (!enableFlowControl) {
      return;
    }

    int current = atomicCurrent.decrementAndGet();

    if (atomicCurrent.get() < 0) {
      LOGGER.info("Current value less that zero because of single record imports, back to zero...");
      atomicCurrent.set(0);
      return;
    }

    LOGGER.info("--------------- Current value after complete event: {} ---------------", current);

    resumeIfThresholdAllows(current);
  }

  private void resumeIfThresholdAllows(int current) {
    if (current <= recordsThreshold) {
      List<KafkaConsumerWrapper<String, String>> rawRecordsReadConsumers = consumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());

      rawRecordsReadConsumers.forEach(consumer -> {
        if (consumer.demand() == 0) {
          consumer.resume();

          LOGGER.info("Kafka consumer - id: {}, subscription - {} is resumed, because {} exceeded {} threshold",
            consumer.getId(), DI_RAW_RECORDS_CHUNK_READ.value(), this.atomicCurrent, recordsThreshold);
        }
      });
    }
  }
}
