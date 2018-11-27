package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.resource.ChangeManager;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.JobExecutionService;
import org.folio.services.JobExecutionServiceImpl;
import org.folio.util.ExceptionHelper;
import org.folio.util.OkapiConnectionParams;

import javax.ws.rs.core.Response;
import java.util.Map;

public class ChangeManagerImpl implements ChangeManager {

  private JobExecutionService jobExecutionService;
  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeManagerImpl.class);

  public ChangeManagerImpl(Vertx vertx, String tenantId) {
    String calculatedTenantId = TenantTool.calculateTenantId(tenantId);
    this.jobExecutionService = new JobExecutionServiceImpl(vertx, calculatedTenantId);
  }

  @Override
  public void postChangeManagerJobExecutions(InitJobExecutionsRqDto initJobExecutionsRqDto, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
        jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
          .map(initJobExecutionsRsDto ->
            (Response) PostChangeManagerJobExecutionsResponse.respond201WithApplicationJson(initJobExecutionsRsDto))
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.error("Error during initializing JobExecution entities", e, e.getMessage());
        asyncResultHandler.handle(Future.failedFuture(e));
      }
    });

  }
}
