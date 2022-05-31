package org.folio.verticle;

import org.folio.kafka.AsyncRecordHandler;
import org.folio.verticle.consumers.DataImportInitKafkaHandler;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;
import org.springframework.stereotype.Component;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INITIALIZATION_STARTED;

@Component
public class DataImportInitConsumersVerticle extends AbstractConsumersVerticle {

  @Autowired
  private DataImportInitKafkaHandler initializationHandler;

  @Override
  public List<String> getEvents() {
    return List.of(DI_INITIALIZATION_STARTED.value());
  }

  @Override
  public AsyncRecordHandler<String, String> getHandler() {
    return initializationHandler;
  }
}
