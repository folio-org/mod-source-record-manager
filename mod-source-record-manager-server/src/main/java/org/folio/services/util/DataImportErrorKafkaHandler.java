package org.folio.services.util;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.rest.jaxrs.model.Event;
import org.folio.services.EventHandlingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Qualifier("DataImportErrorKafkaHandler")
public class DataImportErrorKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataImportErrorKafkaHandler.class);


  private Vertx vertx;
  private EventHandlingService eventHandlingService;

  public DataImportErrorKafkaHandler(@Autowired Vertx vertx, @Autowired EventHandlingService eventHandlingService) {
    this.vertx = vertx;
    this.eventHandlingService = eventHandlingService;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiConnectionParams = fromKafkaHeaders(kafkaHeaders);
    Event event = new JsonObject(record.value()).mapTo(Event.class);

    return eventHandlingService.handle(event.getEventPayload(), okapiConnectionParams)
      .map(v -> record.key())
      .onFailure(Future::failedFuture);
  }


}
