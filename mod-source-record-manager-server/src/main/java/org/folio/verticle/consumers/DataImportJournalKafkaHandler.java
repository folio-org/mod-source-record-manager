package org.folio.verticle.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.services.journal.JournalService;
import org.folio.services.journal.JournalUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Qualifier("DataImportJournalKafkaHandler")
public class DataImportJournalKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LogManager.getLogger();

  private Vertx vertx;
  private JournalService journalService;

  public DataImportJournalKafkaHandler(@Autowired Vertx vertx,
                                       @Autowired @Qualifier("journalServiceProxy")
                                         JournalService journalService) {
    this.vertx = vertx;
    this.journalService = journalService;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    Promise<String> result = Promise.promise();
    List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
    Event event = new JsonObject(record.value()).mapTo(Event.class);
    LOGGER.debug("Event was received: {}", event.getEventType());
    try {
      DataImportEventPayload eventPayload = new ObjectMapper().readValue(ZIPArchiver.unzip(event.getEventPayload()), DataImportEventPayload.class);
      //TODO MODSOURMAN-384
      JournalRecord journalRecord = JournalUtil.buildJournalRecordByEvent(eventPayload, JournalRecord.ActionType.CREATE,
        JournalRecord.EntityType.INSTANCE, JournalRecord.ActionStatus.COMPLETED);
      journalService.save(JsonObject.mapFrom(journalRecord), okapiConnectionParams.getTenantId());
      result.complete();
    } catch (Exception e) {
      LOGGER.error("Error during processing journal event", e);
      result.fail(e);
    }
    return result.future();
  }
}
