package org.folio.verticle.consumers;

import static java.lang.String.format;

import static org.folio.kafka.KafkaHeaderUtils.kafkaHeadersToMap;
import static org.folio.rest.jaxrs.model.SourceRecordState.RecordState.ACTUAL;
import static org.folio.rest.jaxrs.model.SourceRecordState.RecordState.ERROR;

import java.io.IOException;
import java.util.HashMap;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.SourceRecordState;
import org.folio.services.SourceRecordStateService;
import org.folio.verticle.consumers.util.QMEventTypes;

@Component
@Log4j2
@Qualifier("QuickMarcUpdateKafkaHandler")
@RequiredArgsConstructor
public class QuickMarcUpdateKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final String RECORD_ID_KEY = "RECORD_ID";
  private static final String UNZIP_ERROR_MESSAGE = "Error during unzip";
  private static final String EVENT_ID_PREFIX = QuickMarcUpdateKafkaHandler.class.getSimpleName();

  private final KafkaInternalCache kafkaInternalCache;
  private final SourceRecordStateService sourceRecordStateService;
  private final Vertx vertx;

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    var event = Json.decodeValue(record.value(), Event.class);
    String cacheEventId = format("%s-%s", EVENT_ID_PREFIX, event.getId());

    if (!kafkaInternalCache.containsByKey(cacheEventId)) {
      kafkaInternalCache.putToCache(cacheEventId);
      OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(kafkaHeadersToMap(record.headers()), vertx);
      var tenantId = okapiConnectionParams.getTenantId();
      var eventType = event.getEventType();
      return getEventPayload(event)
        .compose(eventPayload -> updateSourceState(eventPayload, eventType, tenantId))
        .compose(s -> Future.succeededFuture(record.key()), th -> {
          log.error("Update record state was failed while handle {} event", eventType);
          return Future.failedFuture(th);
        });
    } else {
      return Future.succeededFuture(record.key());
    }
  }

  private Future<SourceRecordState> updateSourceState(HashMap<String, String> eventPayload, String eventType,
                                                      String tenantId) {
    log.debug("Event was received for {}: {}", eventType, eventPayload);
    var recordId = eventPayload.get(RECORD_ID_KEY);
    if (QMEventTypes.QM_ERROR.name().equals(eventType)) {
      return sourceRecordStateService.updateState(recordId, ERROR, tenantId);
    } else {
      return sourceRecordStateService.updateState(recordId, ACTUAL, tenantId);
    }
  }

  @SuppressWarnings("unchecked")
  private Future<HashMap<String, String>> getEventPayload(Event event) {
    try {
      var eventPayload = Json.decodeValue(ZIPArchiver.unzip(event.getEventPayload()), HashMap.class);
      return Future.succeededFuture(eventPayload);
    } catch (IOException e) {
      log.error(UNZIP_ERROR_MESSAGE, e);
      return Future.failedFuture(UNZIP_ERROR_MESSAGE);
    }
  }
}
