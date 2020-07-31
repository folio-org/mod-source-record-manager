package org.folio.services.util;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.SourceRecordState;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.services.EventHandlingService;
import org.folio.services.SourceRecordStateService;
import org.folio.services.journal.JournalService;
import org.folio.services.journal.JournalUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;

@Component
public class ChangeManagerKafkaHandlers {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeManagerKafkaHandlers.class);
  private static final String INVENTORY_INSTANCE_CREATED_ERROR_MSG = "Failed to process: DI_INVENTORY_INSTANCE_CREATED";
  private static final String UNZIP_ERROR_MESSAGE = "Error during unzip";
  private static final String RECORD_ID_KEY = "RECORD_ID";


  private JournalService journalService;
  private SourceRecordStateService sourceRecordStateService;
  private EventHandlingService recordProcessedEventHandleService;

  public ChangeManagerKafkaHandlers(@Autowired SourceRecordStateService sourceRecordStateService, @Autowired EventHandlingService recordProcessedEventHandleService, @Autowired Vertx vertx) {
    this.sourceRecordStateService = sourceRecordStateService;
    this.recordProcessedEventHandleService = recordProcessedEventHandleService;
    this.journalService = JournalService.createProxy(vertx);
  }

  public Handler<KafkaConsumerRecord<String, String>> postChangeManagerHandlersCreatedInventoryInstance(String tenantId) {
    //TODO: all commits in Kafka Consumers must be manual!
    return record -> {
      String value = record.value();
      try {
        LOGGER.debug("Event was received: {}", value);

        DataImportEventPayload event = ObjectMapperTool.getMapper().readValue(ZIPArchiver.unzip(value), DataImportEventPayload.class);
        JournalRecord journalRecord = JournalUtil.buildJournalRecordByEvent(event, JournalRecord.ActionType.CREATE,
          JournalRecord.EntityType.INSTANCE, JournalRecord.ActionStatus.COMPLETED);
        journalService.save(JsonObject.mapFrom(journalRecord), tenantId);
      } catch (Exception e) {
        LOGGER.error(INVENTORY_INSTANCE_CREATED_ERROR_MSG, e);
      }
    };
  }

  public Handler<KafkaConsumerRecord<String, String>> postChangeManagerHandlersProcessingResult(OkapiConnectionParams params) {
    //TODO: all commits in Kafka Consumers must be manual!
    return record -> {
      String value = record.value();
      recordProcessedEventHandleService.handle(value, params);
    };
  }

  public Handler<KafkaConsumerRecord<String, String>> postChangeManagerHandlersQmCompleted(String tenantId) {
    //TODO: all commits in Kafka Consumers must be manual!
    return record -> {
      String value = record.value();
      try {
        HashMap<String, String> eventPayload = ObjectMapperTool.getMapper().readValue(ZIPArchiver.unzip(value), HashMap.class);
        LOGGER.debug("Event was received for QM_COMPLETE: {}", eventPayload);
        sourceRecordStateService.updateState(eventPayload.get(RECORD_ID_KEY), SourceRecordState.RecordState.ACTUAL, tenantId);
      } catch (IOException e) {
        LOGGER.error(UNZIP_ERROR_MESSAGE, e);
      }
    };
  }

  public Handler<KafkaConsumerRecord<String, String>> postChangeManagerHandlersQmError(String tenantId) {
    //TODO: all commits in Kafka Consumers must be manual!
    return record -> {
      try {
        String value = record.value();
        HashMap<String, String> eventPayload = ObjectMapperTool.getMapper().readValue(ZIPArchiver.unzip(value), HashMap.class);
        LOGGER.debug("Event was received for QM_ERROR: {}", eventPayload);
        sourceRecordStateService.updateState(eventPayload.get(RECORD_ID_KEY), SourceRecordState.RecordState.ERROR, tenantId);
      } catch (IOException e) {
        LOGGER.error(UNZIP_ERROR_MESSAGE, e);
      }
    };
  }
}
