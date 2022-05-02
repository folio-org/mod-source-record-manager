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
public class FlowControlServiceImpl implements FlowControlService {
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

    LOGGER.info("--------------- Current for chunk processed: {} ---------------", current);

    if (current >= maxSimultaneousRecords) {
      List<KafkaConsumerWrapper<String, String>> rawRecordsReadConsumers = consumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());

      rawRecordsReadConsumers.forEach(consumer -> {
        if (consumer.demand() > 0) {
          consumer.pause();

          LOGGER.info("{} kafka consumer - id: {} is paused, because {} exceeded {} max simultaneous records",
            DI_RAW_RECORDS_CHUNK_READ.value(), consumer.getId(), this.atomicCurrent, maxSimultaneousRecords);
        }
      });
    }
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

    LOGGER.info("--------------- Current for DI_COMPLETE/DI_ERROR: {} ---------------", current);

    if (current <= recordsThreshold) {
      List<KafkaConsumerWrapper<String, String>> rawRecordsReadConsumers = consumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());

      rawRecordsReadConsumers.forEach(consumer -> {
        if (consumer.demand() == 0) {
          consumer.resume();

          LOGGER.info("{} kafka consumer - id: {} is resumed, because {} exceeded {} threshold",
            DI_RAW_RECORDS_CHUNK_READ.value(), consumer.getId(), this.atomicCurrent, recordsThreshold);
        }
      });
    }
  }
}
