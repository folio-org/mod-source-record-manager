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

  @Value("${di.flow.max_simultaneous_records:10}")
  private Integer maxSimultaneousRecords;
  @Value("${di.flow.records_threshold:5}")
  private Integer recordsThreshold;
  @Value("${di.flow.control.enable:true}")
  private boolean enableFlowControl;

  @Autowired
  private KafkaConsumersStorage consumersStorage;

  private final AtomicInteger processedRecordsCount = new AtomicInteger(0);

  @PostConstruct
  public void init() {
    LOGGER.info("Flow control feature is {}", enableFlowControl ? "enabled" : "disabled");
  }

  @Override
  public void trackChunkProcessedEvent(String tenantId, Integer initialRecordsSize) {
    if (!enableFlowControl) {
      return;
    }

    if (processedRecordsCount.addAndGet(initialRecordsSize) > maxSimultaneousRecords) {
      List<KafkaConsumerWrapper<String, String>> rawRecordsReadConsumers = consumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());

      rawRecordsReadConsumers.forEach(consumer -> {
        if (consumer.demand() > 0) {
          consumer.pause();

          LOGGER.info("{} kafka consumer is paused caused by {} tenant, because {} exceeded {} max simultaneous records",
            DI_RAW_RECORDS_CHUNK_READ.value(), tenantId, processedRecordsCount, maxSimultaneousRecords);
        }
      });
    }
  }

  @Override
  public void trackRecordCompleteEvent(String tenantId) {
    if (!enableFlowControl) {
      return;
    }

    if (processedRecordsCount.decrementAndGet() <= recordsThreshold) {
      List<KafkaConsumerWrapper<String, String>> rawRecordsReadConsumers = consumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());

      rawRecordsReadConsumers.forEach(consumer -> {
        if (consumer.demand() == 0) {
          consumer.resume();

          LOGGER.info("{} kafka consumer is resumed caused by {} tenant, because {} exceeded {} threshold",
            DI_RAW_RECORDS_CHUNK_READ.value(), tenantId, processedRecordsCount, recordsThreshold);
        }
      });
    }
  }
}
