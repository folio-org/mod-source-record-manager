package org.folio.verticle;

import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.BackPressureGauge;
import org.folio.kafka.ProcessRecordErrorHandler;
import org.folio.services.flowcontrol.RawRecordsFlowControlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Collections;
import java.util.List;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_READ;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class RawMarcChunkConsumersVerticle extends AbstractConsumersVerticle {

  @Value("${di.flow.control.enable:true}")
  private boolean enableFlowControl;

  @Autowired
  @Qualifier("RawMarcChunksKafkaHandler")
  private AsyncRecordHandler<String, String> rawMarcChunksKafkaHandler;

  @Autowired
  @Qualifier("RawMarcChunksErrorHandler")
  private ProcessRecordErrorHandler<String, String> errorHandler;

  @Override
  public List<String> getEvents() {
    return Collections.singletonList(DI_RAW_RECORDS_CHUNK_READ.value());
  }

  @Override
  public AsyncRecordHandler<String, String> getHandler() {
    return this.rawMarcChunksKafkaHandler;
  }

  @Override
  public ProcessRecordErrorHandler<String, String> getErrorHandler() {
    return this.errorHandler;
  }

  @Override
  public BackPressureGauge<Integer, Integer, Integer> getBackPressureGauge() {
    if (!enableFlowControl) {
      return null; // the default back pressure gauge function from folio-kafka-wrapper will be used if flow control feature disabled
    }

    /*
     * Disable back pressure gauge defined by folio-kafka-wrapper by setting this simple implementation. This
     * implementation will not allow folio-kafka-wrapper to pause/resume the topic. Flow control mechanism, defined in
     * this codebase, will be used instead to handle load from DI_RAW_RECORDS_CHUNK_READ topic. Flow control will
     * have exclusive rights to pause/resume the topic.
     * @see RawRecordsFlowControlService
     */
    return (globalLoad, localLoad, threshold) -> localLoad < 0;
  }

}
