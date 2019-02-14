package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.rest.jaxrs.resource.MetadataProvider;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.JobExecutionService;
import org.folio.services.JobExecutionServiceImpl;

import javax.ws.rs.core.Response;
import java.util.Map;

public class MetadataProviderImpl implements MetadataProvider {

  private static final int LANDING_PAGE_LOGS_LIMIT = 25;
  private static final Logger LOGGER = LoggerFactory.getLogger(MetadataProviderImpl.class);
  private static final String FILE_EXTENSION_DUPLICATE_ERROR_CODE = "fileExtension.duplication.invalid";
  private static final String FILE_EXTENSION_VALIDATE_ERROR_MESSAGE = "Failed to validate file extension";

  private JobExecutionService jobExecutionService;
  private Vertx vertx;

  public MetadataProviderImpl(Vertx vertx, String tenantId) {
    this.vertx = vertx;
    String calculatedTenantId = TenantTool.calculateTenantId(tenantId);
    this.jobExecutionService = new JobExecutionServiceImpl(vertx, calculatedTenantId);
  }

  @Override
  public void getMetadataProviderLogs(boolean landingPage, String query, int offset, int limit,
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
