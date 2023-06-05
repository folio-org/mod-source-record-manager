package org.folio.services.flowcontrol;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

@Service
@EnableScheduling
public class RawRecordsFlowControlServiceImpl implements RawRecordsFlowControlService {
  private static final Logger LOGGER = LogManager.getLogger();

  @Value("${di.flow.control.max.simultaneous.chunks:2}")
  private Integer maxSimultaneousChunks;
  @Value("${di.flow.control.records.threshold:25}")
  private Integer recordsThreshold;
  @Value("${di.flow.control.enable:true}")
  private boolean enableFlowControl;

  @Autowired
  private KafkaConsumersStorage consumersStorage;

  /**
   * Represents the current state of flow control to make decisions when need to make new fetch call.
   * It is basically counter, that increasing when DI_RAW_RECORDS_CHUNK_READ event comes and decreasing when
   * DI_COMPLETE, DI_ERROR, duplicated chunks come.
   *
   * When current state below di.flow.control.records.threshold - all raw records consumers from consumer's group should fetch.
   */
  private final Map<String, Integer> currentState = new ConcurrentHashMap<>();

  /**
   * Represents the previous flow control state to decide when to turn on the consumer to get the next chunk of data,
   * in case a module instance has not received any messages in 2 minutes (for example, when the second module
   * instance processed them).
   */
  private final Map<String, Pair<Integer, Integer>> historyState = new ConcurrentHashMap<>();

  @PostConstruct
  public void init() {
    LOGGER.info("init:: Flow control feature is {}", enableFlowControl ? "enabled" : "disabled");
  }

  /**
   * This method schedules resetting state of flow control. By default, it triggered each 2 mins.
   * This is distributed system and count of raw records pushed to process can be not corresponding
   * with DI_COMPLETE/DI_ERROR, for example because of imports are stuck.
   *
   * For example, batch size - 50 records, max simultaneous - 1 batch, records threshold - 25.
   * Flow control receives 50 records (1 batch) and pause consumers, 20 DI_COMPLETE events came, 30 was missed due to some DB issue.
   * Current state would equals to 30 and threshold will never met 24 records so consumers will be paused forever
   * and as a result all subsequent imports will not have a chance to execute.
   *
   * This scheduled method to reset state intended to prevent that. The correlation between max simultaneous and records
   * threshold can be missed during resetting that can cause that resume/pause cycle may not be as usual, because we are
   * starting from clear state after reset, but any events would not be missed and consumers never pause forever.
   */
  @Scheduled(initialDelayString = "PT1M", fixedRateString = "${di.flow.control.reset.state.interval:PT2M}")
  public void resetState() {
    if (!enableFlowControl) {
      return;
    }

    currentState.forEach((tenantId, counterVal) -> {
      LOGGER.info("resetState:: Tenant: [{}] ", tenantId);
      resumeIfThresholdAllows(tenantId);
    });
  }

  @Override
  public void trackChunkReceivedEvent(String tenantId, Integer recordsCount) {
    if (!enableFlowControl) {
      return;
    }

    initFetchMode(tenantId);
    increaseCounterInDb(tenantId, recordsCount);
    LOGGER.debug("trackChunkReceivedEvent:: Tenant: [{}]. Chunk received. Record count: {}, Current state: {} ",
      tenantId, recordsCount, currentState.get(tenantId));
  }

  @Override
  public void trackRecordCompleteEvent(String tenantId, Integer recordsCount) {
    if (!enableFlowControl) {
      return;
    }

    decreaseState(tenantId, recordsCount);
    LOGGER.debug("trackRecordCompleteEvent:: Tenant: [{}]. Record count: {}, Current state:{}",
      tenantId, recordsCount, currentState.get(tenantId));
  }

  @Override
  public void trackChunkDuplicateEvent(String tenantId, Integer recordsCount) {
    if (!enableFlowControl) {
      return;
    }

    decreaseState(tenantId, recordsCount);
    LOGGER.debug("trackChunkDuplicateEvent:: Tenant: [{}]. Record count: {}, Current state:{}",
      tenantId, recordsCount, currentState.get(tenantId));
  }

  private void decreaseState(String tenantId, Integer recordsCount) {
    initFetchMode(tenantId);
    decreaseCounterInDb(tenantId, recordsCount);
    resumeIfThresholdAllows(tenantId);
  }

  private void resumeIfThresholdAllows(String tenantId) {

    LOGGER.info("resumeIfThresholdAllows :: Tenant: [{}]. Current state:{}, History state: {}",
      tenantId, currentState.get(tenantId), historyState.get(tenantId));

    historyState.putIfAbsent(tenantId, Pair.of(0,0));
    if (historyState.get(tenantId).getKey().equals(currentState.get(tenantId))) {
      Integer previousValue = historyState.get(tenantId).getValue();
      historyState.put(tenantId, Pair.of(historyState.get(tenantId).getKey(), ++previousValue));
      LOGGER.debug("resumeIfThresholdAllows :: Tenant: [{}]. Increase counter. Current state:{}, History state: {}",
        tenantId, currentState.get(tenantId), historyState.get(tenantId));
    } else {
      historyState.put(tenantId, Pair.of(currentState.get(tenantId),0));
    }

    consumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value())
      .forEach(consumer -> {
        LOGGER.info("resumeIfThresholdAllows :: Tenant: [{}]. DI_RAW_RECORDS_CHUNK_READ. " +
            "ConsumerId:{}, Demand:{}, Current state:{}, History state: {}",
          tenantId, consumer.getId(), consumer.demand(), currentState.get(tenantId), historyState.get(tenantId));
        if (((consumer.demand() == 0) && (currentState.get(tenantId) < recordsThreshold)) ||
          (historyState.get(tenantId).getKey() > 0 && historyState.get(tenantId).getValue() > 0)) {
          if (consumer.demand() > maxSimultaneousChunks) {
            consumer.pause(); //set demand to 0, because fetch can only add new demand value when demand > 0
          }
          consumer.fetch(maxSimultaneousChunks);

          LOGGER.info("resumeIfThresholdAllows :: Tenant: [{}]. Fetch: DI_RAW_RECORDS_CHUNK_READ. " +
            "ConsumerId:{}, Demand:{}, Current state:{}, History state:{}", tenantId, consumer.getId(), consumer.demand(),
            currentState.get(tenantId), historyState.get(tenantId));

          //counters reset
          historyState.put(tenantId, Pair.of(0, 0));
          currentState.put(tenantId, 0);
        }
      });
  }

  private void initFetchMode(String tenantId) {

    // A read stream is either in "flowing" or "fetch" mode
    // initially the stream is in "flowing" mode
    // when the stream is in "flowing" mode, elements are delivered to the handler
    // when the stream is in "fetch" mode, only the number of requested elements will be delivered to the handler
    if (currentState.containsKey(tenantId) && (currentState.get(tenantId) == 0)) {
      consumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value())
        .forEach(consumer -> {
          consumer.pause();
          consumer.fetch(maxSimultaneousChunks);
        });
    }
  }

  public void increaseCounterInDb(String tenantId, Integer recordsCount) {
    currentState.compute(tenantId, (k, v) -> v == null ? recordsCount : v + recordsCount);
  }

  public void decreaseCounterInDb(String tenantId, Integer recordsCount) {
    currentState.compute(tenantId, (k, v) -> v == null || v < 0 ? 0 : v - recordsCount);
  }

}
