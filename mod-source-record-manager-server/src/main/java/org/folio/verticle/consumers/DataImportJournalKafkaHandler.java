package org.folio.verticle.consumers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.exception.ConflictException;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.rest.jaxrs.model.Event;
import org.folio.services.EventProcessedService;
import org.folio.services.journal.JournalRecordMapperException;
import org.folio.services.journal.JournalService;
import org.folio.verticle.consumers.util.EventTypeHandlerSelector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

import static org.folio.services.RecordsPublishingServiceImpl.RECORD_ID_HEADER;

@Component
@Qualifier("DataImportJournalKafkaHandler")
public class DataImportJournalKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LogManager.getLogger();
  private static final String EVENT_ID_PREFIX = DataImportJournalKafkaHandler.class.getSimpleName();
  public static final String DATA_IMPORT_JOURNAL_KAFKA_HANDLER_UUID = "ebe06a6a-73ca-11ec-90d6-0242ac120003";


  private Vertx vertx;
  private JournalService journalService;
  private EventProcessedService eventProcessedService;
  private EventTypeHandlerSelector eventTypeHandlerSelector;

  public DataImportJournalKafkaHandler(@Autowired Vertx vertx,
                                       @Autowired @Qualifier("eventProcessedService") EventProcessedService eventProcessedService,
                                       @Autowired EventTypeHandlerSelector eventTypeHandlerSelector,
                                       @Autowired @Qualifier("journalServiceProxy") JournalService journalService) {
    this.vertx = vertx;
    this.journalService = journalService;
    this.eventProcessedService = eventProcessedService;
    this.eventTypeHandlerSelector = eventTypeHandlerSelector;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    Promise<String> result = Promise.promise();
    List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
    String recordId = okapiConnectionParams.getHeaders().get(RECORD_ID_HEADER);
    Event event = new JsonObject(record.value()).mapTo(Event.class);
    LOGGER.debug("Event was received with recordId: {} event type: {}", recordId, event.getEventType());
    try {
      eventProcessedService.collectData(DATA_IMPORT_JOURNAL_KAFKA_HANDLER_UUID, event.getId(), okapiConnectionParams.getTenantId())
        .onSuccess(e -> {
          try {
            DataImportEventPayload eventPayload = new ObjectMapper().readValue(event.getEventPayload(), DataImportEventPayload.class);
            eventTypeHandlerSelector.getHandler(eventPayload).handle(journalService, eventPayload, okapiConnectionParams.getTenantId());
            result.complete(record.key());
          } catch (JsonProcessingException | JournalRecordMapperException ex) {
            LOGGER.error("Error during processing journal event", ex);
            result.fail(ex);
          }
        })
        .onFailure(e -> {
          if (e instanceof ConflictException) {
            LOGGER.info(e.getMessage());
            result.complete(record.key());
          } else {
            LOGGER.error("Error during processing data-import result. Database connection error: ", e);
            result.fail(e);
          }
        });
    } catch (Exception e) {
      LOGGER.error("Error during processing journal event", e);
      result.fail(e);
    }
    return result.future();
  }
}
