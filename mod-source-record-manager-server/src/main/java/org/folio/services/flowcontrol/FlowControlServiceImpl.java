package org.folio.services.flowcontrol;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.rest.jaxrs.model.Event;
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

  private static final int DEFAULT_CHUNK_SIZE = 50;

  @Value("${di.flow.max_simultaneous_records:500}")
  private Integer maxSimultaneousRecords;
  @Value("${di.flow.records_threshold:250}")
  private Integer recordsThreshold;
  @Value("${di.flow.control.enable:false}")
  private boolean enableFlowControl;

  @Autowired
  private KafkaConsumersStorage consumersStorage;

  private final AtomicInteger processedRecordsCount = new AtomicInteger(0);

  @PostConstruct
  public void init() {
    LOGGER.info("Flow control feature is {}", enableFlowControl ? "enabled" : "disabled");
  }

  @Override
  public void trackChunkProcessedEvent(Event event) {
    if (!enableFlowControl) {
      return;
    }

    if (processedRecordsCount.addAndGet(DEFAULT_CHUNK_SIZE) > maxSimultaneousRecords) {
      List<KafkaConsumerWrapper<String, String>> rawRecordsReadConsumers = consumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());

      rawRecordsReadConsumers.forEach(KafkaConsumerWrapper::pause);

      LOGGER.debug("{} kafka consumer is paused, because {} exceeded {} max simultaneous records",
        DI_RAW_RECORDS_CHUNK_READ.value(), processedRecordsCount.get(), maxSimultaneousRecords );
    }
  }

  @Override
  public void trackRecordCompleteEvent(Event event) {
    if (!enableFlowControl) {
      return;
    }

    if (processedRecordsCount.decrementAndGet() <= recordsThreshold) {
      List<KafkaConsumerWrapper<String, String>> rawRecordsReadConsumers = consumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());

      rawRecordsReadConsumers.forEach(KafkaConsumerWrapper::resume);

      LOGGER.debug("{} kafka consumer working, because {} exceeded {} threshold",
        DI_RAW_RECORDS_CHUNK_READ.value(), processedRecordsCount.get(), recordsThreshold);
    }
  }
}
