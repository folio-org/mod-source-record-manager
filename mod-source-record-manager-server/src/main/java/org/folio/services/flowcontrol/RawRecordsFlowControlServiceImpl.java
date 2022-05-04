package org.folio.services.flowcontrol;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.verticle.consumers.consumerstorage.KafkaConsumersStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_READ;

@Service
public class RawRecordsFlowControlServiceImpl implements RawRecordsFlowControlService {
  private static final Logger LOGGER = LogManager.getLogger();

  @Value("${di.flow.control.max.simultaneous.records:100}")
  private Integer maxSimultaneousRecords;
  @Value("${di.flow.control.records.threshold:50}")
  private Integer recordsThreshold;
  @Value("${di.flow.control.enable:true}")
  private boolean enableFlowControl;

  @Autowired
  private KafkaConsumersStorage consumersStorage;

  /**
   * Represents the current state of flow control to make decisions when need to make pause/resume calls.
   * It is basically counter, that increasing when DI_RAW_RECORDS_CHUNK_READ event comes and decreasing when
   * DI_COMPLETE, DI_ERROR come.
   *
   * When current state equals or exceeds di.flow_control.max_simultaneous_records - all raw records consumers from consumer's group should pause.
   * When current state equals or below di.flow_control.records_threshold - all raw records consumers from consumer's group should resume.
   */
  private final AtomicInteger currentState = new AtomicInteger(0);

  @PostConstruct
  public void init() {
    LOGGER.info("Flow control feature is {}", enableFlowControl ? "enabled" : "disabled");
  }

  @Override
  public void trackChunkReceivedEvent(Integer initialRecordsCount) {
    if (!enableFlowControl) {
      return;
    }

    int current = currentState.addAndGet(initialRecordsCount);

    LOGGER.info("--------------- Current value after chunk received: {} ---------------", current);

    if (current >= maxSimultaneousRecords) {
      Collection<KafkaConsumerWrapper<String, String>> rawRecordsReadConsumers = consumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());

      rawRecordsReadConsumers.forEach(consumer -> {
        if (consumer.demand() > 0) {
          consumer.pause();

          LOGGER.info("Kafka consumer - id: {}, subscription - {} is paused, because {} exceeded {} max simultaneous records",
            consumer.getId(), DI_RAW_RECORDS_CHUNK_READ.value(), current, maxSimultaneousRecords);
        }
      });
    }
  }

  @Override
  public void trackChunkDuplicateEvent(Integer duplicatedRecordsCount) {
    if (!enableFlowControl) {
      return;
    }

    int prev = currentState.get();

    currentState.set(prev - duplicatedRecordsCount);

    LOGGER.info("--------------- Chunk duplicate event comes, update current value from: {} to: {} ---------------", prev, currentState.get());

    resumeIfThresholdAllows();
  }

  @Override
  public void trackRecordCompleteEvent() {
    if (!enableFlowControl) {
      return;
    }

    int current = currentState.decrementAndGet();

    if (currentState.get() < 0) {
      LOGGER.info("Current value less that zero because of single record imports, back to zero...");
      currentState.set(0);
      return;
    }

    LOGGER.info("--------------- Current value after complete event: {} ---------------", current);

    resumeIfThresholdAllows();
  }

  private void resumeIfThresholdAllows() {
    if (currentState.get() <= recordsThreshold) {
      Collection<KafkaConsumerWrapper<String, String>> rawRecordsReadConsumers = consumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());

      rawRecordsReadConsumers.forEach(consumer -> {
        if (consumer.demand() == 0) {
          consumer.resume();

          LOGGER.info("Kafka consumer - id: {}, subscription - {} is resumed, because {} met threshold {}",
            consumer.getId(), DI_RAW_RECORDS_CHUNK_READ.value(), this.currentState, recordsThreshold);
        }
      });
    }
  }
}
