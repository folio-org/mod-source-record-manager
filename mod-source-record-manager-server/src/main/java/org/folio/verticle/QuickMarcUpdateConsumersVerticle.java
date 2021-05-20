package org.folio.verticle;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;

import org.folio.kafka.AsyncRecordHandler;
import org.folio.verticle.consumers.QuickMarcUpdateKafkaHandler;
import org.folio.verticle.consumers.util.QMEventTypes;

public class QuickMarcUpdateConsumersVerticle extends AbstractConsumersVerticle {

  @Autowired
  private QuickMarcUpdateKafkaHandler quickMarcUpdateKafkaHandler;

  @Override
  public List<String> getEvents() {
    return List.of(QMEventTypes.QM_ERROR.name(), QMEventTypes.QM_INVENTORY_INSTANCE_UPDATED.name());
  }

  @Override
  public AsyncRecordHandler<String, String> getHandler() {
    return this.quickMarcUpdateKafkaHandler;
  }

}
