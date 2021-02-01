package org.folio.verticle;

import org.folio.kafka.AsyncRecordHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.support.AbstractApplicationContext;

import java.util.Arrays;
import java.util.List;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_COMPLETED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;

public class DataImportConsumersVerticle extends AbstractConsumersVerticle {

  @Autowired
  @Qualifier("DataImportKafkaHandler")
  private AsyncRecordHandler<String, String> dataImportKafkaHandler;

  @Override
  public List<String> getEvents() {
    return Arrays.asList(DI_COMPLETED.value(), DI_ERROR.value());
  }

  @Override
  public AsyncRecordHandler<String, String> getHandler() {
    return this.dataImportKafkaHandler;
  }

  //TODO: get rid of this workaround with global spring context
  @Deprecated
  public static void setSpringGlobalContext(AbstractApplicationContext springGlobalContext) {
    DataImportConsumersVerticle.springGlobalContext = springGlobalContext;
  }
}
