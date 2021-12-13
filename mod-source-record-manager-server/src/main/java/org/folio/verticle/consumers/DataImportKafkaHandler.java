package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;

import org.apache.commons.lang3.StringUtils;
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
      Event event = Json.decodeValue(record.value(), Event.class);
      String jobExecutionId = extractJobExecutionId(kafkaHeaders);
      LOGGER.info("Event was received with recordId: '{}' event type: '{}' with jobExecutionId: '{}'", recordId, event.getEventType(), jobExecutionId);

      if (StringUtils.isBlank(recordId)) {
        handleLocalEvent(result, okapiConnectionParams, event);
        return result.future();
      }

      if (kafkaInternalCache.containsByKey(recordId)) {
        return Future.succeededFuture(record.key());
      }

      kafkaInternalCache.putToCache(recordId);
      handleLocalEvent(result, okapiConnectionParams, event);
      return result.future();
    } catch (Exception e) {
      LOGGER.error("Error during processing data-import result", e);
      return Future.failedFuture(e);
    }
  }

  private void handleLocalEvent(Promise<String> result, OkapiConnectionParams okapiConnectionParams, Event event) {
    eventHandlingService.handle(event.getEventPayload(), okapiConnectionParams)
      .onSuccess(ar -> result.complete())
      .onFailure(ar -> {
        LOGGER.error("Error during processing DataImport Result: ", ar.getCause());
        result.fail(ar.getCause());
      });
  }

  private String extractJobExecutionId(List<KafkaHeader> headers) {
    return headers.stream()
      .filter(header -> header.key().equals(RECORD_ID_HEADER))
      .findFirst()
      .map(header -> header.value().toString())
      .orElse(null);
  }
}
