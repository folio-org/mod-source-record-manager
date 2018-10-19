package org.folio.services.repository;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public class MetadataRepositoryImpl implements MetadataRepository {

  private static final String LOGS_STUB_PATH = "ramls/examples/logCollection.sample";
  private static final String JOBS_STUB_PATH = "ramls/examples/jobCollection.sample";
  private static final String LOG_STUB_PATH = "ramls/examples/log.sample";
  private static final String JOB_STUB_PATH = "ramls/examples/jobExecution.sample";
  private static final String FAILED_RESPONSE = "Failed to read sample data";
  private final Vertx vertx;

  public MetadataRepositoryImpl(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public void getLogs(String tenantId, String query, int offset, int limit, boolean landingPage, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    //TODO replace stub response
    vertx.fileSystem().readFile(LOGS_STUB_PATH, event -> {
      if (event.succeeded()) {
        asyncResultHandler.handle(Future.succeededFuture(new JsonObject(event.result())));
      } else {
        asyncResultHandler.handle(Future.failedFuture(FAILED_RESPONSE));
      }
    });
  }

  @Override
  public void getJobs(String tenantId, String query, int offset, int limit, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    //TODO replace stub response
    vertx.fileSystem().readFile(JOBS_STUB_PATH, event -> {
      if (event.succeeded()) {
        asyncResultHandler.handle(Future.succeededFuture(new JsonObject(event.result())));
      } else {
        asyncResultHandler.handle(Future.failedFuture(FAILED_RESPONSE));
      }
    });
  }

  @Override
  public void createLog(String tenantId, JsonObject log, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    //TODO replace stub response
    vertx.fileSystem().readFile(LOG_STUB_PATH, event -> {
      if (event.succeeded()) {
        asyncResultHandler.handle(Future.succeededFuture(new JsonObject(event.result())));
      } else {
        asyncResultHandler.handle(Future.failedFuture(FAILED_RESPONSE));
      }
    });
  }

  @Override
  public void updateLog(String tenantId, JsonObject log, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    //TODO replace stub response
    vertx.fileSystem().readFile(LOG_STUB_PATH, event -> {
      if (event.succeeded()) {
        asyncResultHandler.handle(Future.succeededFuture(new JsonObject(event.result())));
      } else {
        asyncResultHandler.handle(Future.failedFuture(FAILED_RESPONSE));
      }
    });
  }

  @Override
  public void getLogById(String tenantId, String logId, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    //TODO replace stub response
    vertx.fileSystem().readFile(LOG_STUB_PATH, event -> {
      if (event.succeeded()) {
        asyncResultHandler.handle(Future.succeededFuture(new JsonObject(event.result())));
      } else {
        asyncResultHandler.handle(Future.failedFuture(FAILED_RESPONSE));
      }
    });
  }

  @Override
  public void deleteLogById(String tenantId, String logId, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    //TODO replace stub response
    asyncResultHandler.handle(Future.succeededFuture());
  }

  @Override
  public void createJob(String tenantId, JsonObject job, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    //TODO replace stub response
    vertx.fileSystem().readFile(JOB_STUB_PATH, event -> {
      if (event.succeeded()) {
        asyncResultHandler.handle(Future.succeededFuture(new JsonObject(event.result())));
      } else {
        asyncResultHandler.handle(Future.failedFuture(FAILED_RESPONSE));
      }
    });
  }

  @Override
  public void updateJob(String tenantId, JsonObject job, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    //TODO replace stub response
    vertx.fileSystem().readFile(JOB_STUB_PATH, event -> {
      if (event.succeeded()) {
        asyncResultHandler.handle(Future.succeededFuture(new JsonObject(event.result())));
      } else {
        asyncResultHandler.handle(Future.failedFuture(FAILED_RESPONSE));
      }
    });
  }

  @Override
  public void getJobById(String tenantId, String jobId, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    //TODO replace stub response
    vertx.fileSystem().readFile(JOB_STUB_PATH, event -> {
      if (event.succeeded()) {
        asyncResultHandler.handle(Future.succeededFuture(new JsonObject(event.result())));
      } else {
        asyncResultHandler.handle(Future.failedFuture(FAILED_RESPONSE));
      }
    });
  }

  @Override
  public void deleteJobById(String tenantId, String jobId, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    //TODO replace stub response
    asyncResultHandler.handle(Future.succeededFuture());
  }

}
