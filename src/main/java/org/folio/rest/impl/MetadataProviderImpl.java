package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.rest.jaxrs.model.JobCollection;
import org.folio.rest.jaxrs.model.LogCollection;
import org.folio.rest.jaxrs.resource.SourceRecordManager;
import org.folio.services.provider.MetadataService;
import org.folio.util.SourceRecordManagerConstants;

import javax.ws.rs.core.Response;
import java.util.Map;

public class MetadataProviderImpl implements SourceRecordManager {

  private final Logger logger = LoggerFactory.getLogger(MetadataProviderImpl.class);

  private final MetadataService metadataService;
  private final String tenantId;

  public MetadataProviderImpl(Vertx vertx, String tenantId){
    this.tenantId = tenantId;
    this.metadataService = MetadataService.createProxy(vertx, SourceRecordManagerConstants.METADATA_SERVICE_ADDRESS);
  }

  @Override
  public void getSourceRecordManagerLogs(String query, int offset, int limit, boolean landingPage,
                                         Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                                         Context vertxContext) {
    try {
      vertxContext.runOnContext(v -> metadataService.getLogs(tenantId, query, offset, limit, landingPage, reply -> {
        if (reply.succeeded()) {
          LogCollection logs = reply.result().mapTo(LogCollection.class);
          asyncResultHandler.handle(
            Future.succeededFuture(GetSourceRecordManagerLogsResponse.respond200WithApplicationJson(logs)));
        } else {
          String message = "Failed to get logs";
          logger.error(message, reply.cause());
          asyncResultHandler.handle(
            Future.succeededFuture(GetSourceRecordManagerLogsResponse.respond500WithTextPlain(message)));
        }
      }));
    } catch (Exception e) {
      logger.error("Error running on verticle for getSourceRecordManagerLogs: " + e.getMessage(), e);
      asyncResultHandler.handle(Future.succeededFuture(
        GetSourceRecordManagerLogsResponse.respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
    }
  }

  @Override
  public void getSourceRecordManagerJobs(String query, int offset, int limit, Map<String, String> okapiHeaders,
                                         Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      vertxContext.runOnContext(v -> metadataService.getJobs(tenantId, query, offset, limit, reply -> {
        if (reply.succeeded()) {
          JobCollection jobs = reply.result().mapTo(JobCollection.class);
          asyncResultHandler.handle(
            Future.succeededFuture(GetSourceRecordManagerJobsResponse.respond200WithApplicationJson(jobs)));
        } else {
          String message = "Failed to get jobs";
          logger.error(message, reply.cause());
          asyncResultHandler.handle(
            Future.succeededFuture(GetSourceRecordManagerJobsResponse.respond500WithTextPlain(message)));
        }
      }));
    } catch (Exception e) {
      logger.error("Error running on verticle for getSourceRecordManagerJobs: " + e.getMessage(), e);
      asyncResultHandler.handle(Future.succeededFuture(
        GetSourceRecordManagerJobsResponse.respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
    }
  }
}
