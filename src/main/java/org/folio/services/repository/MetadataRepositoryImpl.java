package org.folio.services.repository;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang.StringUtils;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.jaxrs.model.Log;

import java.time.LocalDateTime;
import java.util.List;

public class MetadataRepositoryImpl implements MetadataRepository {

  private static final String LOGS_STUB_PATH = "ramls/examples/logCollection.sample";
  private static final String JOBS_STUB_PATH = "ramls/examples/jobExecutionCollection.sample";
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
  public void getJobExecutions(String tenantId, String query, int offset, int limit, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    //TODO replace stub response

    vertx.fileSystem().readFile(JOBS_STUB_PATH, response -> {
      if (response.succeeded()) {

        JobExecutionCollection jobExecutionCollection = new JsonObject(response.result()).mapTo(JobExecutionCollection.class);
        List<JobExecution> jobExecutionList = jobExecutionCollection.getJobExecutions();

        JobExecutionCollection resultJobExecutionCollection = new JobExecutionCollection();
        for (JobExecution jobExecution : jobExecutionList) {
          if (StringUtils.isBlank(query) || (!StringUtils.isBlank(query) && query.contains(jobExecution.getStatus().toString()))) {
            resultJobExecutionCollection.getJobExecutions().add(jobExecution);
          }
        }

        for (JobExecution jobExecution : resultJobExecutionCollection.getJobExecutions()) {
          if (JobExecution.Status.RUNNING.equals(jobExecution.getStatus())) {
            jobExecution.setStartedDate(LocalDateTime.now().toString());
          }
        }
        resultJobExecutionCollection.setTotalRecords(resultJobExecutionCollection.getJobExecutions().size());
        asyncResultHandler.handle(Future.succeededFuture(JsonObject.mapFrom(resultJobExecutionCollection)));
      } else {
        asyncResultHandler.handle(Future.failedFuture(FAILED_RESPONSE));
      }
    });
  }

  @Override
  public void createLog(String tenantId, JsonObject log, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    //TODO replace stub response
    try {
      Log mappedLog = log.mapTo(Log.class);
      asyncResultHandler.handle(Future.succeededFuture(JsonObject.mapFrom(mappedLog)));
    } catch (Exception e) {
      asyncResultHandler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public void updateLog(String tenantId, JsonObject log, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    //TODO replace stub response
    try {
      Log mappedLog = log.mapTo(Log.class);
      asyncResultHandler.handle(Future.succeededFuture(JsonObject.mapFrom(mappedLog)));
    } catch (Exception e) {
      asyncResultHandler.handle(Future.failedFuture(e));
    }
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
  public void createJobExecution(String tenantId, JsonObject jobExecution, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    //TODO replace stub response
    try {
      JobExecution mappedJobExecution = jobExecution.mapTo(JobExecution.class);
      asyncResultHandler.handle(Future.succeededFuture(JsonObject.mapFrom(mappedJobExecution)));
    } catch (Exception e) {
      asyncResultHandler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public void updateJobExecution(String tenantId, JsonObject jobExecution, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    //TODO replace stub response
    try {
      JobExecution mappedJobExecution = jobExecution.mapTo(JobExecution.class);
      asyncResultHandler.handle(Future.succeededFuture(JsonObject.mapFrom(mappedJobExecution)));
    } catch (Exception e) {
      asyncResultHandler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public void getJobExecutionById(String tenantId, String jobExecutionId, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
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
  public void deleteJobExecutionById(String tenantId, String jobExecutionId, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    //TODO replace stub response
    asyncResultHandler.handle(Future.succeededFuture());
  }

}
