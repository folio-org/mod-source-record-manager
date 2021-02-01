package org.folio.verticle;

import org.folio.kafka.AsyncRecordHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.support.AbstractApplicationContext;

import java.util.Collections;
import java.util.List;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_MARC_BIB_RECORDS_CHUNK_READ;

public class RawMarcChunkConsumersVerticle extends AbstractConsumersVerticle {

  @Autowired
  @Qualifier("RawMarcChunksKafkaHandler")
  private AsyncRecordHandler<String, String> rawMarcChunksKafkaHandler;

  @Override
  public List<String> getEvents() {
    return Collections.singletonList(DI_RAW_MARC_BIB_RECORDS_CHUNK_READ.value());
  }

  @Override
  public AsyncRecordHandler<String, String> getHandler() {
    return this.rawMarcChunksKafkaHandler;
  }

  //TODO: get rid of this workaround with global spring context
  @Deprecated
  public static void setSpringGlobalContext(AbstractApplicationContext springGlobalContext) {
    RawMarcChunkConsumersVerticle.springGlobalContext = springGlobalContext;
  }

}
