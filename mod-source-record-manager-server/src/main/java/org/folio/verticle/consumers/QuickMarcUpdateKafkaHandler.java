package org.folio.verticle.consumers;

import static org.folio.kafka.KafkaHeaderUtils.kafkaHeadersToMap;
import static org.folio.verticle.consumers.util.QMEventTypes.QM_COMPLETED;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.rest.jaxrs.model.Event;
import org.folio.services.QuickMarcEventProducerService;
import org.folio.verticle.consumers.util.QmCompletedEventPayload;

@Component
@Log4j2
@Qualifier("QuickMarcUpdateKafkaHandler")
@RequiredArgsConstructor
public class QuickMarcUpdateKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final String RECORD_ID_KEY = "RECORD_ID";
  private static final String ERROR_KEY = "ERROR";

  private final QuickMarcEventProducerService producerService;
  private final Vertx vertx;

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    var event = Json.decodeValue(record.value(), Event.class);
    var eventType = event.getEventType();

    var kafkaHeaders = record.headers();
    var okapiConnectionParams = OkapiConnectionParams.createSystemUserConnectionParams(
      kafkaHeadersToMap(kafkaHeaders), vertx);
    var tenantId = okapiConnectionParams.getTenantId();

    return getEventPayload(event)
      .compose(eventPayload -> sendQmCompletedEvent(eventPayload, tenantId, kafkaHeaders))
      .compose(s -> Future.succeededFuture(record.key()), th -> {
        log.warn("handle:: Update record state was failed while handle {} event", eventType);
        return Future.failedFuture(th);
      });
  }

  private Future<Map<String, String>> sendQmCompletedEvent(Map<String, String> eventPayload,
                                                           String tenantId, List<KafkaHeader> kafkaHeaders) {
    var recordId = eventPayload.get(RECORD_ID_KEY);
    var errorMessage = eventPayload.get(ERROR_KEY);
    var qmCompletedEventPayload = new QmCompletedEventPayload(recordId, errorMessage);
    return producerService.sendEvent(Json.encode(qmCompletedEventPayload), QM_COMPLETED.name(), null, tenantId, kafkaHeaders)
      .map(v -> eventPayload);
  }

  @SuppressWarnings("unchecked")
  private Future<Map<String, String>> getEventPayload(Event event) {
    try {
      var eventPayload = Json.decodeValue(event.getEventPayload(), HashMap.class);
      return Future.succeededFuture(eventPayload);
    } catch (Exception e) {
      return Future.failedFuture(e);
    }
  }
}
