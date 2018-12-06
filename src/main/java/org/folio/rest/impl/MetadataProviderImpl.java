package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import org.folio.rest.jaxrs.resource.MetadataProvider;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.JobExecutionService;
import org.folio.services.JobExecutionServiceImpl;
import org.folio.util.ExceptionHelper;

import javax.ws.rs.core.Response;
import java.util.Map;

public class MetadataProviderImpl implements MetadataProvider {

  public static final int LANDING_PAGE_LOGS_LIMIT = 25;

  private JobExecutionService jobExecutionService;

  public MetadataProviderImpl(Vertx vertx, String tenantId) {
    String calculatedTenantId = TenantTool.calculateTenantId(tenantId);
    this.jobExecutionService = new JobExecutionServiceImpl(vertx, calculatedTenantId);
  }

  @Override
  public void getMetadataProviderLogs(String query, int offset, int limit, boolean landingPage,
                                      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                                      Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        jobExecutionService.getLogCollectionDtoByQuery(query, offset, landingPage ? LANDING_PAGE_LOGS_LIMIT : limit)
          .map(GetMetadataProviderLogsResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(
          ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getMetadataProviderJobExecutions(String query, int offset, int limit, Map<String, String> okapiHeaders,
                                               Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {

    vertxContext.runOnContext(v -> {
      try {
        jobExecutionService.getJobExecutionCollectionDtoByQuery(query, offset, limit)
          .map(GetMetadataProviderJobExecutionsResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(
          ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }
}
