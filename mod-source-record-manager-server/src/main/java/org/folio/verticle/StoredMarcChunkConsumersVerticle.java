package org.folio.verticle;

import org.folio.kafka.AsyncRecordHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.support.AbstractApplicationContext;

import java.util.Collections;
import java.util.List;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_PARSED_MARC_BIB_RECORDS_CHUNK_SAVED;

public class StoredMarcChunkConsumersVerticle extends AbstractConsumersVerticle {

  @Autowired
  @Qualifier("StoredMarcChunksKafkaHandler")
  private AsyncRecordHandler<String, String> storedMarcChunksKafkaHandler;

  @Override
  public List<String> getEvents() {
    return Collections.singletonList(DI_PARSED_MARC_BIB_RECORDS_CHUNK_SAVED.value());
  }

  @Override
  public AsyncRecordHandler<String, String> getHandler() {
    return this.storedMarcChunksKafkaHandler;
  }

  //TODO: get rid of this workaround with global spring context
  @Deprecated
  public static void setSpringGlobalContext(AbstractApplicationContext springGlobalContext) {
    StoredMarcChunkConsumersVerticle.springGlobalContext = springGlobalContext;
  }

}
