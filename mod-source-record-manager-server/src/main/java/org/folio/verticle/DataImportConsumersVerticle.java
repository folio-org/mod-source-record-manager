package org.folio.verticle;

import org.folio.kafka.AsyncRecordHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Arrays;
import java.util.List;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_COMPLETED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;

/**
 * Verticle to process DI_COMPLETE, DI_ERROR events.
 * Marked with SCOPE_PROTOTYPE to support deploying more than 1 instance.
 * @see org.folio.rest.impl.InitAPIImpl
 */
@Component
@Scope(SCOPE_PROTOTYPE)
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

}
