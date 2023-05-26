package org.folio.services.flowcontrol;

import io.vertx.kafka.client.consumer.impl.KafkaConsumerImpl;
import org.apache.commons.collections4.map.MultiKeyMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.rest.persist.PostgresClient;
import org.folio.services.EventProcessedService;
import org.folio.verticle.consumers.consumerstorage.KafkaConsumersStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;
import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_READ;

@Service
@EnableScheduling
public class RawRecordsFlowControlServiceImpl implements RawRecordsFlowControlService {
  private static final Logger LOGGER = LogManager.getLogger();

  @Value("${di.flow.control.max.simultaneous.records:50}")
  private Integer maxSimultaneousRecords;
  @Value("${di.flow.control.records.threshold:25}")
  private Integer recordsThreshold;
  @Value("${di.flow.control.enable:true}")
  private boolean enableFlowControl;

  @Autowired
  private KafkaConsumersStorage consumersStorage;
  @Autowired
  private EventProcessedService eventProcessedService;

  /**
   * Represents the current state of flow control to make decisions when need to make pause/resume calls.
   * It is basically counter, that increasing when DI_RAW_RECORDS_CHUNK_READ event comes and decreasing when
   * DI_COMPLETE, DI_ERROR come.
   *
   * When current state equals or exceeds di.flow.control.max.simultaneous.records - all raw records consumers from consumer's group should pause.
   * When current state equals or below di.flow.control.records.threshold - all raw records consumers from consumer's group should resume.
   */
  private final Map<String, Integer> currentState = new ConcurrentHashMap<>();
  //private final Map<String, Pair<Integer, Integer>> previousState = new ConcurrentHashMap<>();

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
  @Scheduled(initialDelayString = "PT1S", fixedRateString = "${di.flow.control.reset.state.interval:PT1S}")
  public void resetState() {
    if (!enableFlowControl) {
      return;
    }

//    Collection<KafkaConsumerWrapper<String, String>> rawRecordsReadConsumers = consumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());
//    for (String key : currentState.keySet()) {
//      if (previousState.get(key) == null) {
//        previousState.put(key, Pair.of(currentState.get(key), 0));
//      } else {
//        if (Objects.equals(previousState.get(key).getKey(), currentState.get(key))) {
//          previousState.put(key, Pair.of(currentState.get(key), previousState.get(key).getValue() + 1));
//        }
//      }
//
//      if (currentState.get(key) > maxSimultaneousRecords) {
//        LOGGER.info("--------------- resetState:: Tenant: [{}]. Pause. Current state: {}. ---------------", key, currentState.get(key));
//        rawRecordsReadConsumers.forEach(consumer -> {
//          if ((consumer.demand() > 0)) {
//            consumer.pause();
//          }
//        });
//      } else {
//        if ((currentState.get(key) <= 0) || (currentState.get(key) / recordsThreshold == 0) ||
//          previousState.get(key).getValue() >= 10)  {
//          LOGGER.info("--------------- resetState:: Tenant: [{}]. Resume. Current state: {}. Pause cycles: {} ---------------",
//            key, currentState.get(key), previousState.get(key).getValue());
//          previousState.remove(key);
//          rawRecordsReadConsumers.forEach(consumer -> {
//            if (consumer.demand() == 0) {
//              consumer.resume();
//            }
//          });
//        } else {
//          LOGGER.info("--------------- resetState:: Tenant: [{}]. Consumers on pause. Current state: {}. ---------------", key, currentState.get(key));
//        }
//      }
//    }

    if (currentState.values().stream().anyMatch(counter -> counter > 0)) {
      //currentState.replaceAll((tenantId, oldValue) -> 0);
      //LOGGER.info("resetState:: State has been reset to initial value, current value: 0");
      LOGGER.info("--------------- resetState:: The try to reset state. ---------------");

      for (String key : currentState.keySet()) {
        if ((currentState.get(key) <= 0) || (currentState.get(key) / recordsThreshold == 0)) {
          LOGGER.info("--------------- resetState:: Tenant: [{}]. Resume. Current state: {}. ---------------", key, currentState.get(key));
          Collection<KafkaConsumerWrapper<String, String>> rawRecordsReadConsumers = consumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());
          rawRecordsReadConsumers.forEach(consumer -> {
            if (consumer.demand() == 0) {
              consumer.resume();
            }
          });
        } else {
          LOGGER.info("--------------- resetState:: Tenant: [{}]. Consumers on pause. Current state: {}. ---------------", key, currentState.get(key));
        }
      }
    }

    currentState.forEach((tenantId, counterVal) -> resumeIfThresholdAllows(tenantId));
  }

  @Override
  public void trackChunkReceivedEvent(String tenantId, Integer recordsCount) {
    if (!enableFlowControl) {
      return;
    }
    LOGGER.info("--------------- trackChunkReceivedEvent:: Tenant: [{}]. Chunk received. Records: {} ---------------", tenantId, recordsCount);
    increaseCounterInDb(tenantId, recordsCount);

    if (currentState.get(tenantId) > maxSimultaneousRecords) {
      Collection<KafkaConsumerWrapper<String, String>> rawRecordsReadConsumers = consumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());
      LOGGER.info("trackChunkReceivedEvent :: Tenant: [{}]. consumers.size: {}", tenantId, rawRecordsReadConsumers.size());
      rawRecordsReadConsumers.forEach(consumer -> {
        LOGGER.info("trackChunkReceivedEvent :: Tenant: [{}]. Pause:: ConsumerId: {}, Demand:{}", tenantId, consumer.getId(), consumer.demand());
        if (consumer.demand() > 0) {
          consumer.pause();
          LOGGER.info("Tenant: [{}]. Kafka consumer - id: {}, subscription - {} is paused, because {} exceeded {} max simultaneous records",
            tenantId, consumer.getId(), DI_RAW_RECORDS_CHUNK_READ.value(), currentState.get(tenantId), maxSimultaneousRecords);
        }
      });
    } else {
      LOGGER.info("--------------- trackChunkReceivedEvent:: Tenant: [{}]. Consumers are resume. Current state: {} ---------------",
        tenantId, currentState.get(tenantId));
    }
  }

  @Override
  public void trackChunkDuplicateEvent(String tenantId, Integer recordsCount) {
    if (!enableFlowControl) {
      return;
    }

    LOGGER.info("--------------- Tenant: [{}]. Chunk duplicate event comes. Duplicate Records: {} ---------------", tenantId, recordsCount);
    decreaseCounterInDb(tenantId, recordsCount);
    resumeIfThresholdAllows(tenantId);
  }

  @Override
  public void trackRecordCompleteEvent(String tenantId, Integer recordsCount) {
    if (!enableFlowControl) {
      return;
    }

    LOGGER.info("trackRecordCompleteEvent:: Tenant: [{}]. Completed Records: {}", tenantId, recordsCount);
    decreaseCounterInDb(tenantId, recordsCount);
    resumeIfThresholdAllows(tenantId);
  }

  private void resumeIfThresholdAllows(String tenantId) {
    LOGGER.info("resumeIfThresholdAllows:: Tenant: [{}]. Try to resume. Current state: {}", tenantId, currentState.get(tenantId));
    if ((currentState.get(tenantId) <= 0) || (currentState.get(tenantId) / recordsThreshold == 0)) {
      Collection<KafkaConsumerWrapper<String, String>> rawRecordsReadConsumers = consumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());
      LOGGER.info("resumeIfThresholdAllows:: Tenant: [{}]. rawRecordsReadConsumers:: size: {}", tenantId, rawRecordsReadConsumers.size());
      rawRecordsReadConsumers.forEach(consumer -> {
        LOGGER.info("resumeIfThresholdAllows:: Tenant: [{}]. Resume:: consumerId: {}, demand:{}", tenantId, consumer.getId(), consumer.demand());
        if (consumer.demand() == 0) {
          //consumer.resume();
          consumer.fetch(maxSimultaneousRecords);
          LOGGER.info("resumeIfThresholdAllows:: Tenant: [{}]. Kafka consumer - id: {}, subscription - {} is resumed for all tenants, because {} met threshold {}",
            tenantId, consumer.getId(), DI_RAW_RECORDS_CHUNK_READ.value(), this.currentState, recordsThreshold);
        }
      });
    } else {
      LOGGER.info("--------------- resumeIfThresholdAllows:: Tenant: [{}]. Consumers on pause. Current state: {}. ---------------",
        tenantId, currentState.get(tenantId));
    }
  }

  public void increaseCounterInDb(String tenantId, Integer recordsCount) {
    LOGGER.info("--------------- increaseCounterInDb:: Tenant: [{}]. Increase on: {} ---------------", tenantId, recordsCount);
    currentState.compute(tenantId, (k, v) -> v == null ? recordsCount : v + recordsCount);
    LOGGER.info("--------------- increaseCounterInDb:: Tenant: [{}]. Current state: {} ---------------", tenantId, currentState.get(tenantId));
  }

  public void decreaseCounterInDb(String tenantId, Integer recordsCount) {
    LOGGER.info("--------------- decreaseCounterInDb:: Tenant: [{}]. Decrease on: {} ---------------", tenantId, recordsCount);
    currentState.compute(tenantId, (k, v) -> v == null || v < 0 ? 0 : v - recordsCount);
    LOGGER.info("--------------- decreaseCounterInDb:: Tenant: [{}]. Current state: {} ---------------", tenantId, currentState.get(tenantId));
  }

}
