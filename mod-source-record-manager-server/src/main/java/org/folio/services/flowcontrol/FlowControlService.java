package org.folio.services.flowcontrol;

import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.rest.jaxrs.model.Event;
import org.folio.verticle.DataImportConsumersVerticle;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_READ;

import java.util.concurrent.atomic.AtomicInteger;

@Service
public class FlowControlService implements IFlowControlService {
  @Value("${di.flow.max_simultaneous_records:500}")
  private Integer maxSimultaneousRecords;
  @Value("${di.flow.records_threshold:250}")
  private Integer recordsThreshold;

  private AtomicInteger diCompletedAndErrorCount;

  @Override
  public void trackEvent(Event event) {
    if (diCompletedAndErrorCount.incrementAndGet() > maxSimultaneousRecords) {
      KafkaConsumerWrapper<String, String> rawRecordsReadConsumer = DataImportConsumersVerticle.getKafkaConsumerByEvent(DI_RAW_RECORDS_CHUNK_READ.value());
      rawRecordsReadConsumer.pause();
    }

  }
}
