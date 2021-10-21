package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.rest.jaxrs.model.Event;
import org.folio.services.EventHandlingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

import static org.folio.services.RecordsPublishingServiceImpl.CORRELATION_ID_HEADER;

@Component
@Qualifier("DataImportKafkaHandler")
public class DataImportKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LogManager.getLogger();
  static final String RECORD_ID_HEADER = "recordId";

  private Vertx vertx;
  private EventHandlingService eventHandlingService;
  private KafkaInternalCache kafkaInternalCache;

  public DataImportKafkaHandler(@Autowired Vertx vertx,
                                @Autowired EventHandlingService eventHandlingService,
                                @Autowired KafkaInternalCache kafkaInternalCache) {
    this.vertx = vertx;
    this.eventHandlingService = eventHandlingService;
    this.kafkaInternalCache = kafkaInternalCache;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    try {
      Promise<String> result = Promise.promise();
      List<KafkaHeader> kafkaHeaders = record.headers();
      OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
      String recordId = okapiConnectionParams.getHeaders().get(RECORD_ID_HEADER);
      Event event = new JsonObject(record.value()).mapTo(Event.class);
      LOGGER.info("Event was received with recordId: {} event type: {}", recordId, event.getEventType());

      if (kafkaInternalCache.containsByKey(recordId)) {
        return Future.succeededFuture(record.key());
      }

      kafkaInternalCache.putToCache(recordId);
      eventHandlingService.handle(event.getEventPayload(), okapiConnectionParams)
        .onSuccess(ar -> result.complete())
        .onFailure(ar -> {
          LOGGER.error("Error during processing DataImport Result: ", ar.getCause());
          result.fail(ar.getCause());
        });
      return result.future();
    } catch (Exception e) {
      LOGGER.error("Error during processing data-import result", e);
      return Future.failedFuture(e);
    }
  }
}
