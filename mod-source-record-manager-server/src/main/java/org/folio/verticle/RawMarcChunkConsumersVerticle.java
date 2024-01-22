package org.folio.verticle;

import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.BackPressureGauge;
import org.folio.kafka.ProcessRecordErrorHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Collections;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_READ;

/**
 * Verticle to process raw chunks.
 * Marked with SCOPE_PROTOTYPE to support deploying more than 1 instance.
 * @see org.folio.rest.impl.InitAPIImpl
 */
@Component
@Scope(SCOPE_PROTOTYPE)
public class RawMarcChunkConsumersVerticle extends AbstractConsumersVerticle<String, byte[]> {

  @Value("${di.flow.control.enable:true}")
  private boolean enableFlowControl;

  @Autowired
  @Qualifier("RawMarcChunksKafkaHandler")
  private AsyncRecordHandler<String, byte[]> rawMarcChunksKafkaHandler;

  @Autowired
  @Qualifier("RawMarcChunksErrorHandler")
  private ProcessRecordErrorHandler<String, byte[]> errorHandler;

  @Override
  public List<String> getEvents() {
    return Collections.singletonList(DI_RAW_RECORDS_CHUNK_READ.value());
  }

  @Override
  public AsyncRecordHandler<String, byte[]> getHandler() {
    return this.rawMarcChunksKafkaHandler;
  }

  @Override
  public ProcessRecordErrorHandler<String, byte[]> getErrorHandler() {
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

  @Override
  public Boolean shouldAddToGlobalLoad() {
    /*
    since flow control will handle back pressure for this consumer, do not include its messages in the global load
    counts.
     */
    return false;
  }

  @Override
  public String getDeserializerClass() {
    return "org.apache.kafka.common.serialization.ByteArrayDeserializer";
  }

}
