package org.folio.services.provider;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.services.repository.MetadataRepository;
import org.folio.util.SourceRecordManagerConstants;

/**
 * Implementation of MetadataService, calls MetadataRepository to access metadata
 */
public class MetadataServiceImpl implements MetadataService {

  private final Logger logger = LoggerFactory.getLogger(MetadataServiceImpl.class);

  private MetadataRepository metadataRepository;

  public MetadataServiceImpl(Vertx vertx) {
    this.metadataRepository = MetadataRepository.createProxy(vertx, SourceRecordManagerConstants.METADATA_REPOSITORY_ADDRESS);
  }

  @Override
  public void getLogs(String tenantId, String query, int offset, int limit, boolean landingPage, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    try {
      metadataRepository.getLogs(tenantId, query, offset, limit, landingPage, asyncResultHandler);
    } catch (Exception e) {
      logger.error("Error querying metadata repository", e);
      asyncResultHandler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public void getJobExecutions(String tenantId, String query, int offset, int limit, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    try {
      metadataRepository.getJobExecutions(tenantId, query, offset, limit, asyncResultHandler);
    } catch (Exception e) {
      logger.error("Error querying metadata repository", e);
      asyncResultHandler.handle(Future.failedFuture(e));
    }
  }
}
