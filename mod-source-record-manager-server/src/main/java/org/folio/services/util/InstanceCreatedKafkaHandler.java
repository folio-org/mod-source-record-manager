package org.folio.services.util;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.logging.log4j.util.Strings;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.services.EventHandlingService;
import org.folio.services.journal.JournalService;
import org.folio.services.journal.JournalUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Component
@Qualifier("InstanceCreatedKafkaHandler")
public class InstanceCreatedKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LoggerFactory.getLogger(InstanceCreatedKafkaHandler.class);


  private Vertx vertx;
  private EventHandlingService eventHandlingService;
  private JournalService journalService;

  public InstanceCreatedKafkaHandler(@Autowired Vertx vertx, @Autowired EventHandlingService eventHandlingService) {
    this.vertx = vertx;
    this.eventHandlingService = eventHandlingService;
    this.journalService = JournalService.createProxy(vertx);
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiConnectionParams = fromKafkaHeaders(kafkaHeaders);
    Event event = new JsonObject(record.value()).mapTo(Event.class);
    try {
      org.folio.DataImportEventPayload eventPayload = new JsonObject(ZIPArchiver.unzip(event.getEventPayload())).mapTo(DataImportEventPayload.class);
      JournalRecord journalRecord = JournalUtil.buildJournalRecordByEvent(eventPayload, JournalRecord.ActionType.CREATE,
        JournalRecord.EntityType.INSTANCE, JournalRecord.ActionStatus.COMPLETED);
      journalService.save(JsonObject.mapFrom(journalRecord), okapiConnectionParams.getTenantId());

      return eventHandlingService.handle(event.getEventPayload(), okapiConnectionParams)
        .map(v -> record.key())
        .onFailure(Future::failedFuture);

    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.error("Can't process the kafka record: ", e);
      return Future.failedFuture(e);
    }
  }

  //TODO: utility method must be moved out from here
  private OkapiConnectionParams fromKafkaHeaders(List<KafkaHeader> headers) {
    Map<String, String> okapiHeaders = headers
      .stream()
      .collect(Collectors.groupingBy(KafkaHeader::key,
        Collectors.reducing(Strings.EMPTY,
          header -> {
            Buffer value = header.value();
            return Objects.isNull(value) ? "" : value.toString();
          },
          (a, b) -> Strings.isNotBlank(a) ? a : b)));

    return new OkapiConnectionParams(okapiHeaders, vertx);
  }

}
