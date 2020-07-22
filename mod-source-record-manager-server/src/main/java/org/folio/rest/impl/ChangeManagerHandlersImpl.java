package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.SourceRecordState;
import org.folio.rest.jaxrs.resource.ChangeManagerHandlers;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.services.EventHandlingService;
import org.folio.services.SourceRecordStateService;
import org.folio.services.journal.JournalService;
import org.folio.services.journal.JournalUtil;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class ChangeManagerHandlersImpl implements ChangeManagerHandlers {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeManagerHandlersImpl.class);
  private static final String INVENTORY_INSTANCE_CREATED_ERROR_MSG = "Failed to process: DI_INVENTORY_INSTANCE_CREATED";
  private static final String UNZIP_ERROR_MESSAGE = "Error during unzip";
  private static final String RECORD_ID_KEY = "RECORD_ID";

  private JournalService journalService;

  @Autowired
  private SourceRecordStateService sourceRecordStateService;

  @Autowired
  private EventHandlingService recordProcessedEventHandleService;
  private String tenantId;

  public ChangeManagerHandlersImpl(Vertx vertx, String tenantId) { //NOSONAR
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.journalService = JournalService.createProxy(vertx);
    this.tenantId = tenantId;
  }

  @Override
  public void postChangeManagerHandlersCreatedInventoryInstance(String entity, Map<String, String> okapiHeaders,
                                                                Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        LOGGER.debug("Event was received: {}", entity);
        asyncResultHandler.handle(Future.succeededFuture(
          ChangeManagerHandlers.PostChangeManagerHandlersCreatedInventoryInstanceResponse.respond204()));
        DataImportEventPayload event = ObjectMapperTool.getMapper().readValue(ZIPArchiver.unzip(entity), DataImportEventPayload.class);
        JournalRecord journalRecord = JournalUtil.buildJournalRecordByEvent(event, JournalRecord.ActionType.CREATE,
          JournalRecord.EntityType.INSTANCE, JournalRecord.ActionStatus.COMPLETED);
        journalService.save(JsonObject.mapFrom(journalRecord), tenantId);
      } catch (Exception e) {
        LOGGER.error(INVENTORY_INSTANCE_CREATED_ERROR_MSG, e);
      }
    });
  }

  @Override
  public void postChangeManagerHandlersProcessingResult(String entity, Map<String, String> okapiHeaders,
                                                        Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      LOGGER.debug("Event was received: {}", entity);
      asyncResultHandler.handle(Future.succeededFuture(
        ChangeManagerHandlers.PostChangeManagerHandlersProcessingResultResponse.respond204()));
      OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
      recordProcessedEventHandleService.handle(entity, params);
    });
  }

  @Override
  public void postChangeManagerHandlersQmCompleted(String entity, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        HashMap<String, String> eventPayload = ObjectMapperTool.getMapper().readValue(ZIPArchiver.unzip(entity), HashMap.class);
        LOGGER.debug("Event was received for QM_COMPLETE: {}", eventPayload);
        sourceRecordStateService.updateState(eventPayload.get(RECORD_ID_KEY), SourceRecordState.RecordState.ACTUAL, tenantId);
      } catch (IOException e) {
        LOGGER.error(UNZIP_ERROR_MESSAGE, e);
      } finally {
        asyncResultHandler.handle(Future.succeededFuture(
          ChangeManagerHandlers.PostChangeManagerHandlersQmCompletedResponse.respond204()));
      }
    });
  }

  @Override
  public void postChangeManagerHandlersQmError(String entity, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        HashMap<String, String> eventPayload = ObjectMapperTool.getMapper().readValue(ZIPArchiver.unzip(entity), HashMap.class);
        LOGGER.debug("Event was received for QM_ERROR: {}", eventPayload);
        sourceRecordStateService.updateState(eventPayload.get(RECORD_ID_KEY), SourceRecordState.RecordState.ERROR, tenantId);
      } catch (IOException e) {
        LOGGER.error(UNZIP_ERROR_MESSAGE, e);
      } finally {
        asyncResultHandler.handle(Future.succeededFuture(
          ChangeManagerHandlers.PostChangeManagerHandlersQmErrorResponse.respond204()));
      }
    });
  }
}
