package org.folio.verticle;

import org.folio.kafka.AsyncRecordHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Collections;
import java.util.List;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;

public class DataImportJournalConsumersVerticle extends AbstractConsumersVerticle {

  @Autowired
  @Qualifier("DataImportJournalKafkaHandler")
  private AsyncRecordHandler<String, String> dataImportJournalKafkaHandler;

  //TODO MODSOURMAN-384
  @Override
  public List<String> getEvents() {
    return Collections.singletonList(DI_INVENTORY_INSTANCE_CREATED.value());
  }

  @Override
  public AsyncRecordHandler<String, String> getHandler() {
    return this.dataImportJournalKafkaHandler;
  }
}
