package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.rest.jaxrs.model.Event;
import org.folio.services.EventHandlingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Qualifier("DataImportKafkaHandler")
public class DataImportKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LoggerFactory.getLogger(DataImportKafkaHandler.class);

  private Vertx vertx;
  private EventHandlingService eventHandlingService;

  public DataImportKafkaHandler(@Autowired Vertx vertx,
                                @Autowired EventHandlingService eventHandlingService) {
    this.vertx = vertx;
    this.eventHandlingService = eventHandlingService;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    Promise<String> result = Promise.promise();
    List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
    Event event = new JsonObject(record.value()).mapTo(Event.class);

    LOGGER.debug("Event was received: {}", event.getEventType());

    eventHandlingService.handle(event.getEventPayload(), okapiConnectionParams)
      .onSuccess(ar -> result.complete())
      .onFailure(ar -> {
        LOGGER.error("Error during processing DataImport Result: ", ar.getCause());
        result.fail(ar.getCause());
      });
    return result.future();
  }
}
