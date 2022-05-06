package org.folio.services.flowcontrol;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.verticle.consumers.consumerstorage.KafkaConsumersStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_READ;

@Service
@EnableScheduling
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
   * When current state equals or exceeds di.flow.control.max.simultaneous.records - all raw records consumers from consumer's group should pause.
   * When current state equals or below di.flow.control.records.threshold - all raw records consumers from consumer's group should resume.
   */
  private final AtomicInteger currentState = new AtomicInteger(0);

  @PostConstruct
  public void init() {
    LOGGER.info("Flow control feature is {}", enableFlowControl ? "enabled" : "disabled");
  }

  /**
   * This method schedules resetting state of flow control. By default it triggered each 5 mins.
   * This is distributed system and count of raw records pushed to process can be not corresponding
   * with DI_COMPLETE/DI_ERROR, for example because of imports are stuck.
   *
   * For example, batch size - 10, max simultaneous - 20, records threshold - 10.
   * Flow control receives 20 records and pause consumers, 5 DI_COMPLETE events came, 15 was missed due to some DB issue.
   * Current state would equals to 15 and threshold will never met 10 records so consumers will be paused forever
   * and as a result all subsequent imports will not have a chance to execute.
   *
   * This scheduled method to reset state intended to prevent that. The correlation between max simultaneous and records
   * threshold can be missed during resetting that can cause that resume/pause cycle may not be as usual, because we are
   * starting from clear state after reset, but any events would not be missed and consumers never pause forever.
   */
  @Scheduled(cron = "${di.flow.control.reset.state.cron:0 0/5 * * * ?}")
  public void resetState() {
    if (!enableFlowControl) {
      return;
    }

    currentState.getAndUpdate(prev -> {
      if (prev != 0) {
        LOGGER.info("State has been reset to initial value, current value: 0");
        return 0;
      } else {
        return prev;
      }
    });
    resumeIfThresholdAllows();
  }

  @Override
  public void trackChunkReceivedEvent(Integer recordsCount) {
    if (!enableFlowControl) {
      return;
    }

    int current = currentState.addAndGet(recordsCount);

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
  public void trackChunkDuplicateEvent(Integer recordsCount) {
    if (!enableFlowControl) {
      return;
    }

    currentState.getAndUpdate(prev -> {
      int res = prev - recordsCount;
      LOGGER.info("--------------- Chunk duplicate event comes, update current value from: {} to: {} ---------------", prev, res);
      return res;
    });

    resumeIfThresholdAllows();
  }

  @Override
  public void trackRecordCompleteEvent() {
    if (!enableFlowControl) {
      return;
    }

    int current = currentState.decrementAndGet();

    currentState.getAndUpdate(prev -> {
      if (prev < 0) {
        LOGGER.info("Current value less that zero because of single record imports or resetting state, back to zero...");
        return 0;
      } else {
        LOGGER.info("--------------- Current value after complete event: {} ---------------", current);
        return prev;
      }
    });

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
