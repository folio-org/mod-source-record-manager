package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.resource.ChangeManagerHandlers;

import javax.ws.rs.core.Response;
import java.util.Map;

public class ChangeManagerHandlersImpl implements ChangeManagerHandlers {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeManagerHandlersImpl.class);

  public ChangeManagerHandlersImpl(Vertx vertx, String tenantId) { //NOSONAR
  }

  @Override
  public void postChangeManagerHandlersCreatedInventoryInstance(String entity, Map<String, String> okapiHeaders,
                                                                Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        // endpoint will be implemented in https://issues.folio.org/browse/MODSOURMAN-241
        LOGGER.info("Event was received: {}", entity);
        Future.succeededFuture((Response) ChangeManagerHandlers.PostChangeManagerHandlersCreatedInventoryInstanceResponse.respond200())
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.error("Failed to handle event", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
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
}
