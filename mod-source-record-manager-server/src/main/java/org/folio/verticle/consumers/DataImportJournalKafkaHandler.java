package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import lombok.extern.log4j.Log4j2;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.services.EventProcessedService;
import org.folio.services.journal.JournalService;
import org.folio.verticle.consumers.util.EventTypeHandlerSelector;
import org.folio.util.JournalEvent;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

import static org.folio.services.RecordsPublishingServiceImpl.RECORD_ID_HEADER;
import static org.folio.services.util.EventHandlingUtil.extractJobExecutionId;
import static org.folio.services.util.EventHandlingUtil.extractRecordId;

@Log4j2
@Component
@Qualifier("DataImportJournalKafkaHandler")
public class DataImportJournalKafkaHandler implements AsyncRecordHandler<String, byte[]> {
  
  public static final String DATA_IMPORT_JOURNAL_KAFKA_HANDLER_UUID = "ca0c6c56-e74e-4921-b4c9-7b2de53c43ec";

  private final JournalService journalService;
  private final EventProcessedService eventProcessedService;
  private final EventTypeHandlerSelector eventTypeHandlerSelector;

  public DataImportJournalKafkaHandler(EventProcessedService eventProcessedService,
                                       EventTypeHandlerSelector eventTypeHandlerSelector,
                                       @Qualifier("journalServiceProxy") JournalService journalService) {
    this.journalService = journalService;
    this.eventProcessedService = eventProcessedService;
    this.eventTypeHandlerSelector = eventTypeHandlerSelector;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, byte[]> record) {
    try {
      Promise<String> result = Promise.promise();
      List<KafkaHeader> kafkaHeaders = record.headers();
      ConnectionParams okapiConnectionParams = ConnectionParams.createSystemUserConnectionParams(
        KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders));
      String recordId = okapiConnectionParams.getHeaders().get(RECORD_ID_HEADER);
      JournalEvent event = DatabindCodec.mapper().readValue(record.value(), JournalEvent.class);
      log.debug("handle:: Event was received with recordId: {} event type: {}", recordId, event.getEventType());

      eventProcessedService.collectData(DATA_IMPORT_JOURNAL_KAFKA_HANDLER_UUID, event.getId(), okapiConnectionParams.getTenantId())
        .onSuccess(res -> processJournalEvent(result, record, event, okapiConnectionParams.getTenantId()))
        .onFailure(e -> processDeduplicationFailure(result, record, event, e));

      return result.future();
    } catch (Exception e) {
      log.warn("handle:: Error during processing event for data-import journal", e);
      return Future.failedFuture(e);
    }
  }

  private void processJournalEvent(Promise<String> result, KafkaConsumerRecord<String, byte[]> record, JournalEvent event, String tenantId) {
    try {
      DataImportEventPayload eventPayload = event.getEventPayload();
      eventTypeHandlerSelector.getHandler(eventPayload).handle(journalService, eventPayload, tenantId);
      result.complete(record.key());
    } catch (Exception e) {
      log.warn("processJournalEvent:: Error during processing journal event", e);
      result.fail(e);
    }
  }

  private void processDeduplicationFailure(Promise<String> result, KafkaConsumerRecord<String, byte[]> record, JournalEvent event, Throwable e) {
    String jobExecutionId = extractJobExecutionId(record.headers());
    String recordId = extractRecordId(record.headers());
    if (e instanceof DuplicateEventException) { // duplicate coming, ignore it
      log.info(e.getMessage());
      result.complete(record.key());
    } else {
      log.warn("processDeduplicationFailure:: Error with database during collecting of deduplication info for handlerId: {} eventId: {} jobExecutionId: {} recordId: {}",
        DATA_IMPORT_JOURNAL_KAFKA_HANDLER_UUID, event.getId(), jobExecutionId, recordId, e);
      result.fail(e);
    }
  }
}
