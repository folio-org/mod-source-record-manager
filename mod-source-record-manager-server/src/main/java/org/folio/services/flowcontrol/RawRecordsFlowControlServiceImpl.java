package org.folio.services.flowcontrol;

import org.apache.commons.lang3.tuple.Pair;
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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_READ;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_PARSED;

@Service
@EnableScheduling
public class RawRecordsFlowControlServiceImpl implements RawRecordsFlowControlService {
  private static final Logger LOGGER = LogManager.getLogger();

  @Value("${di.flow.control.max.simultaneous.chunks:2}")
  private Integer maxSimultaneousRecords;
  @Value("${di.flow.control.records.threshold:25}")
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
   * When current state equals or exceeds di.flow.control.max.simultaneous.chunks - all raw records consumers from consumer's group should pause.
   * When current state equals or below di.flow.control.records.threshold - all raw records consumers from consumer's group should resume.
   */
  private final Map<String, Integer> currentState = new ConcurrentHashMap<>();
  private final Map<String, Pair<Integer, Integer>> historyState = new ConcurrentHashMap<>();

  @PostConstruct
  public void init() {
    LOGGER.info("init:: Flow control feature is {}", enableFlowControl ? "enabled" : "disabled");
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
  @Scheduled(initialDelayString = "PT3M", fixedRateString = "${di.flow.control.reset.state.interval:PT2M}")
  public void resetState() {
    if (!enableFlowControl) {
      return;
    }

    LOGGER.info("resetState !!!");
    consumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value())
      .forEach(consumer -> {
          consumer.resume();
          LOGGER.info("resetState :: Resume :: DI_RAW_RECORDS_CHUNK_READ. " +
            "ConsumerId:{}, Demand:{}, Load limit:{}", consumer.getId(), consumer.demand(), consumer.getLoadLimit());
      });

//    currentState.forEach((tenantId, counterVal) -> {
//      LOGGER.info("resetState:: Tenant: [{}] ", tenantId);
//      resumeIfThresholdAllows(tenantId);
//    });
  }

  @Override
  public void trackChunkReceivedEvent(String tenantId, Integer recordsCount) {
    if (!enableFlowControl) {
      return;
    }

    increaseCounterInDb(tenantId, recordsCount);
    LOGGER.info("trackChunkReceivedEvent:: Tenant: [{}]. Chunk received. Record count: {}, Current state: {}",
      tenantId, recordsCount, currentState.get(tenantId));

    consumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value())
      .forEach(consumer -> {
        consumer.pause();
        LOGGER.info("trackChunkReceivedEvent :: Pause :: DI_RAW_RECORDS_CHUNK_READ. " +
          "ConsumerId:{}, Demand:{}, Load limit:{}", consumer.getId(), consumer.demand(), consumer.getLoadLimit());
      });
  }

  @Override
  public void trackChunkDuplicateEvent(String tenantId, Integer recordsCount) {
    if (!enableFlowControl) {
      return;
    }

    decreaseCounterInDb(tenantId, recordsCount);
    LOGGER.info("trackChunkDuplicateEvent:: Tenant: [{}]. Chunk duplicate event comes. Duplicate Records: {}, Current state: {}",
      tenantId, recordsCount, currentState.get(tenantId));
    //resumeIfThresholdAllows(tenantId);
  }

  @Override
  public void trackRecordCompleteEvent(String tenantId, Integer recordsCount) {
    if (!enableFlowControl) {
      return;
    }

    decreaseCounterInDb(tenantId, recordsCount);
    LOGGER.info("trackRecordCompleteEvent:: Tenant: [{}]. Record count: {}, Current state:{}",
      tenantId, recordsCount, currentState.get(tenantId));
    //resumeIfThresholdAllows(tenantId);
  }

  private void resumeIfThresholdAllows(String tenantId) {

    historyState.putIfAbsent(tenantId, Pair.of(0,0));
    if (historyState.get(tenantId).getKey().equals(currentState.get(tenantId))) {
      Integer previousValue = historyState.get(tenantId).getValue();
      historyState.put(tenantId, Pair.of(historyState.get(tenantId).getKey(), ++previousValue));
      LOGGER.info("resumeIfThresholdAllows :: Tenant: [{}]. Current state:{}, History state: {}",
        tenantId, currentState.get(tenantId), historyState.get(tenantId));
    } else {
      historyState.put(tenantId, Pair.of(currentState.get(tenantId),0));
    }

    consumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_PARSED.value())
      .forEach(
        consumer -> {
          LOGGER.info("resumeIfThresholdAllows :: Tenant: [{}]. DI_RAW_RECORDS_CHUNK_PARSED. " +
            "ConsumerId:{}, Demand:{}, Load limit:{}", tenantId, consumer.getId(), consumer.demand(), consumer.getLoadLimit());
        }
      );

    consumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value())
      .forEach(consumer -> {
        LOGGER.info("resumeIfThresholdAllows :: Tenant: [{}]. DI_RAW_RECORDS_CHUNK_READ. " +
            "ConsumerId:{}, Demand:{}, Current state:{}, History state: {}",
          tenantId, consumer.getId(), consumer.demand(), currentState.get(tenantId), historyState.get(tenantId));
        if (((consumer.demand() == 0) && (currentState.get(tenantId) < recordsThreshold)) || historyState.get(tenantId).getValue() > 0) {
//          if (consumer.demand() > maxSimultaneousRecords) {
//            consumer.pause();
//          }
//          consumer.fetch(maxSimultaneousRecords);
 //         consumer.resume();
//          consumer.pause();

          LOGGER.info("resumeIfThresholdAllows :: Tenant: [{}]. Fetch: DI_RAW_RECORDS_CHUNK_READ. " +
            "ConsumerId: {}, Demand:{}, History state: {}", tenantId, consumer.getId(), consumer.demand(), historyState.get(tenantId));

          historyState.put(tenantId, Pair.of(0, 0));
          currentState.put(tenantId, 0);
        }
      });
  }

  public void increaseCounterInDb(String tenantId, Integer recordsCount) {
    currentState.compute(tenantId, (k, v) -> v == null ? recordsCount : v + recordsCount);
  }

  public void decreaseCounterInDb(String tenantId, Integer recordsCount) {
    currentState.compute(tenantId, (k, v) -> v == null || v < 0 ? 0 : v - recordsCount);
  }

}
