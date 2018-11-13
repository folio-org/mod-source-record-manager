package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.jaxrs.model.JobExecutionCollectionDto;
import org.folio.rest.jaxrs.model.LogCollection;
import org.folio.rest.jaxrs.resource.MetadataProvider;
import org.folio.services.converters.jobExecution.JobExecutionCollectionToDtoConverter;
import org.folio.services.provider.MetadataService;
import org.folio.util.SourceRecordManagerConstants;

import javax.ws.rs.core.Response;
import java.util.Map;

public class MetadataProviderImpl implements MetadataProvider {

  private final Logger logger = LoggerFactory.getLogger(MetadataProviderImpl.class);

  private final MetadataService metadataService;
  private final String tenantId;
  private JobExecutionCollectionToDtoConverter jobExecutionConverter;

  public MetadataProviderImpl(Vertx vertx, String tenantId) {
    this.tenantId = tenantId;
    this.metadataService = MetadataService.createProxy(vertx, SourceRecordManagerConstants.METADATA_SERVICE_ADDRESS);
    this.jobExecutionConverter = new JobExecutionCollectionToDtoConverter();
  }

  @Override
  public void getMetadataProviderLogs(String query, int offset, int limit, boolean landingPage,
                                      Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                                      Context vertxContext) {
    try {
      vertxContext.runOnContext(v -> metadataService.getLogs(tenantId, query, offset, limit, landingPage, reply -> {
        if (reply.succeeded()) {
          LogCollection logs = reply.result().mapTo(LogCollection.class);
          asyncResultHandler.handle(
            Future.succeededFuture(GetMetadataProviderLogsResponse.respond200WithApplicationJson(logs)));
        } else {
          String message = "Failed to get logs";
          logger.error(message, reply.cause());
          asyncResultHandler.handle(
            Future.succeededFuture(GetMetadataProviderLogsResponse.respond500WithTextPlain(message)));
        }
      }));
    } catch (Exception e) {
      logger.error("Error running on verticle for getMetadataProviderLogs: " + e.getMessage(), e);
      asyncResultHandler.handle(Future.succeededFuture(
        GetMetadataProviderLogsResponse.respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
    }
  }

  @Override
  public void getMetadataProviderJobExecutions(String query, int offset, int limit, Map<String, String> okapiHeaders,
                                               Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      vertxContext.runOnContext(v -> metadataService.getJobExecutions(tenantId, query, offset, limit, reply -> {
        if (reply.succeeded()) {
          JobExecutionCollection jobExecutionCollection = reply.result().mapTo(JobExecutionCollection.class);
          JobExecutionCollectionDto jobExecutionCollectionDto = jobExecutionConverter.convert(jobExecutionCollection);
          asyncResultHandler.handle(
            Future.succeededFuture(GetMetadataProviderJobExecutionsResponse.respond200WithApplicationJson(jobExecutionCollectionDto)));
        } else {
          String message = "Failed to get jobExecutions";
          logger.error(message, reply.cause());
          asyncResultHandler.handle(
            Future.succeededFuture(GetMetadataProviderJobExecutionsResponse.respond500WithTextPlain(message)));
        }
      }));
    } catch (Exception e) {
      logger.error("Error running on verticle for getMetadataProviderJobExecutions: " + e.getMessage(), e);
      asyncResultHandler.handle(Future.succeededFuture(
        GetMetadataProviderJobExecutionsResponse.respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
    }
  }
}
