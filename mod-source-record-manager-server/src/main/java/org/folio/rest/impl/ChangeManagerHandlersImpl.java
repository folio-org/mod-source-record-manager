package org.folio.rest.impl;

import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.resource.ChangeManagerHandlers;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.rest.util.OkapiConnectionParams;
import org.folio.services.journal.JournalRecordMapperException;
import org.folio.services.journal.JournalService;
import org.folio.services.journal.JournalUtil;
import org.folio.util.pubsub.PubSubClientUtils;

import javax.ws.rs.core.Response;

import java.io.IOException;
import java.util.Map;

public class ChangeManagerHandlersImpl implements ChangeManagerHandlers {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeManagerHandlersImpl.class);
  private static final String INVENTORY_INSTANCE_CREATED_ERROR_MSG = "Failed to process: DI_INVENTORY_INSTANCE_CREATED";

  private JournalService journalService;

  public ChangeManagerHandlersImpl(Vertx vertx, String tenantId) { //NOSONAR
    this.journalService = JournalService.createProxy(vertx);
  }

  @Override
  public void postChangeManagerHandlersCreatedInventoryInstance(String entity, Map<String, String> okapiHeaders,
                                                                Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        Future.succeededFuture((Response) ChangeManagerHandlers.PostChangeManagerHandlersCreatedInventoryInstanceResponse.respond200())
          .setHandler(asyncResultHandler);
        LOGGER.info("Event was received: {}", entity);
        DataImportEventPayload event = ObjectMapperTool.getMapper().readValue(entity, DataImportEventPayload.class);
        JournalRecord journalRecord = JournalUtil.buildJournalRecordByEvent(event, JournalRecord.ActionType.CREATE);
        journalService.saveJournalRecord(JsonObject.mapFrom(journalRecord),
          new OkapiConnectionParams(okapiHeaders, vertxContext.owner()).getTenantId());
      } catch (IOException | JournalRecordMapperException e) {
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
        LOGGER.error("Failed to handle event", e);
        Event errorEvent = buildErrorEvent(okapiHeaders.get(OKAPI_TENANT_HEADER), INVENTORY_INSTANCE_CREATED_ERROR_MSG);
        publishEvent(okapiHeaders, asyncResultHandler, vertxContext, errorEvent);
      }
    });
  }

  @Override
  public void postChangeManagerHandlersDataImportError(String entity, Map<String, String> okapiHeaders,
                                                       Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        // endpoint will be implemented in https://issues.folio.org/browse/MODSOURMAN-244
        LOGGER.info("Event was received: {}", entity);
        Future.succeededFuture((Response) ChangeManagerHandlers.PostChangeManagerHandlersDataImportErrorResponse.respond200())
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.error("Failed to handle event", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  private Event buildErrorEvent(String okapiHeaders, String eventPayload) {
    return new Event().withEventType("DI_ERROR")
      .withEventPayload(eventPayload)
      .withEventMetadata(new EventMetadata()
        .withPublishedBy("mod-source-record-manager")
        .withTenantId(okapiHeaders)
        .withEventTTL(1));
  }

  private void publishEvent(Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext, Event event) {
    OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
    PubSubClientUtils.sendEventMessage(event, params)
      .whenComplete((result, throwable) -> {
        if (result) {
          LOGGER.info("Event published successfully: {} ", event.getEventPayload());
          asyncResultHandler.handle(Future.succeededFuture());
        } else {
          LOGGER.error("Failed to publish event: {}", event.getEventPayload());
          asyncResultHandler.handle(Future.failedFuture("Failed to publish event"));
        }
      });
  }
}
