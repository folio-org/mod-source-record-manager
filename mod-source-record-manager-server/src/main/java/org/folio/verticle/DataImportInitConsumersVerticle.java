package org.folio.verticle;

import org.folio.kafka.AsyncRecordHandler;
import org.folio.verticle.consumers.DataImportInitKafkaHandler;
import org.springframework.beans.factory.annotation.Autowired;
import java.util.List;

import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INITIALIZATION_STARTED;

/**
 * Verticle to initialize DI process.
 * Marked with SCOPE_PROTOTYPE to support deploying more than 1 instance.
 * @see org.folio.rest.impl.InitAPIImpl
 */
@Component
@Scope(SCOPE_PROTOTYPE)
public class DataImportInitConsumersVerticle extends AbstractConsumersVerticle<String, String> {

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
