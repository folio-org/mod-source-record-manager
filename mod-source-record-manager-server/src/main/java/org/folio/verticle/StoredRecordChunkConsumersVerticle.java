package org.folio.verticle;

import org.folio.kafka.AsyncRecordHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Arrays;
import java.util.List;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_PARSED_MARC_BIB_RECORDS_CHUNK_SAVED;

public class StoredRecordChunkConsumersVerticle extends AbstractConsumersVerticle {

  @Autowired
  @Qualifier("StoredRecordChunksKafkaHandler")
  private AsyncRecordHandler<String, String> storedRecordChunksKafkaHandler;

  @Override
  public List<String> getEvents() {
    return Arrays.asList(
      DI_PARSED_MARC_BIB_RECORDS_CHUNK_SAVED.value()
    );
  }

  @Override
  public AsyncRecordHandler<String, String> getHandler() {
    return this.storedRecordChunksKafkaHandler;
  }

}
