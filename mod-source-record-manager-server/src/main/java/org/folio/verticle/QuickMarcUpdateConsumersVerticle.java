package org.folio.verticle;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;

import org.folio.kafka.AsyncRecordHandler;
import org.folio.verticle.consumers.QuickMarcUpdateKafkaHandler;
import org.folio.verticle.consumers.util.QMEventTypes;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import static org.springframework.beans.factory.config.BeanDefinition.SCOPE_PROTOTYPE;

/**
 * Verticle to update quick mark.
 * Marked with SCOPE_PROTOTYPE to support deploying more than 1 instance.
 * @see org.folio.rest.impl.InitAPIImpl
 */
@Component
@Scope(SCOPE_PROTOTYPE)
public class QuickMarcUpdateConsumersVerticle extends AbstractConsumersVerticle {

  @Autowired
  private QuickMarcUpdateKafkaHandler quickMarcUpdateKafkaHandler;

  @Override
  public List<String> getEvents() {
    return List.of(
      QMEventTypes.QM_ERROR.name(),
      QMEventTypes.QM_INVENTORY_INSTANCE_UPDATED.name(),
      QMEventTypes.QM_INVENTORY_HOLDINGS_UPDATED.name(),
      QMEventTypes.QM_INVENTORY_AUTHORITY_UPDATED.name()
    );
  }

  @Override
  public AsyncRecordHandler<String, String> getHandler() {
    return this.quickMarcUpdateKafkaHandler;
  }

}
