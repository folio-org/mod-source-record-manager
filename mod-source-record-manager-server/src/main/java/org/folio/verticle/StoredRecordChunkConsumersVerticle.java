package org.folio.verticle;

import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.ProcessRecordErrorHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Collections;
import java.util.List;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_PARSED_RECORDS_CHUNK_SAVED;
import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;

/**
 * Verticle to process stored record chunks.
 * Marked with SCOPE_PROTOTYPE to support deploying more than 1 instance.
 * @see org.folio.rest.impl.InitAPIImpl
 */
@Component
@Scope(SCOPE_PROTOTYPE)
public class StoredRecordChunkConsumersVerticle extends AbstractConsumersVerticle<String, byte[]> {

  @Autowired
  @Qualifier("StoredRecordChunksKafkaHandler")
  private AsyncRecordHandler<String, byte[]> storedRecordChunksKafkaHandler;

  @Autowired
  @Qualifier("StoredRecordChunksErrorHandler")
  private ProcessRecordErrorHandler<String, byte[]> errorHandler;

  @Override
  public List<String> getEvents() {
    return Collections.singletonList(DI_PARSED_RECORDS_CHUNK_SAVED.value());
  }

  @Override
  public AsyncRecordHandler<String, byte[]> getHandler() {
    return this.storedRecordChunksKafkaHandler;
  }

  @Override
  public ProcessRecordErrorHandler<String, byte[]> getErrorHandler() {
    return this.errorHandler;
  }

  @Override
  public String getDeserializerClass() {
    return "org.apache.kafka.common.serialization.ByteArrayDeserializer";
  }
}
