package org.folio.verticle;

import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.ProcessRecordErrorHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Collections;
import java.util.List;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_PARSED_RECORDS_CHUNK_SAVED;

public class StoredRecordChunkConsumersVerticle extends AbstractConsumersVerticle {

  @Autowired
  @Qualifier("StoredRecordChunksKafkaHandler")
  private AsyncRecordHandler<String, String> storedRecordChunksKafkaHandler;

  @Autowired
  @Qualifier("StoredRecordChunksErrorHandler")
  private ProcessRecordErrorHandler<String, String> errorHandler;

  @Override
  public List<String> getEvents() {
    return Collections.singletonList(DI_PARSED_RECORDS_CHUNK_SAVED.value());
  }

  @Override
  public AsyncRecordHandler<String, String> getHandler() {
    return this.storedRecordChunksKafkaHandler;
  }

  @Override
  public ProcessRecordErrorHandler<String, String> getErrorHandler() {
    return this.errorHandler;
  }
}
