package org.folio.services.repository;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.jaxrs.model.Log;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;

import java.util.List;
import java.util.UUID;

public class MetadataRepositoryImpl implements MetadataRepository {

  private final Logger logger = LoggerFactory.getLogger(MetadataRepositoryImpl.class);

  private static final String JOB_EXECUTIONS_TABLE_NAME = "job_executions";
  private static final String JOB_EXECUTION_ID_FIELD = "jobExecutionId";
  private static final String JOB_EXECUTION_ID_JSONB_FIELD = "'jobExecutionId'";

  private static final String LOGS_STUB_PATH = "ramls/examples/logCollection.sample";
  private static final String LOG_STUB_PATH = "ramls/examples/log.sample";
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
    try {
      CQLWrapper cql = getCQLWrapper(query, limit, offset);
      String[] fieldList = {"*"};
      PostgresClient.getInstance(vertx, tenantId).get(JOB_EXECUTIONS_TABLE_NAME, JobExecution.class, fieldList, cql, true, false, getReply -> {
        if (getReply.failed()) {
          logger.error("Error while querying the db to get all JobExecutions", getReply.cause());
          asyncResultHandler.handle(Future.failedFuture(getReply.cause()));
        } else {
          JobExecutionCollection jobExecutionCollection = new JobExecutionCollection();
          List<JobExecution> jobExecutionList = (List<JobExecution>) getReply.result().getResults();
          jobExecutionCollection.setJobExecutions(jobExecutionList);
          jobExecutionCollection.setTotalRecords(jobExecutionList.size());
          asyncResultHandler.handle(Future.succeededFuture(JsonObject.mapFrom(jobExecutionCollection)));
        }
      });
    } catch (Exception e) {
      logger.error("Error while getting all JobExecutions", e);
      asyncResultHandler.handle(Future.failedFuture(e));
    }
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
    try {
      String id = UUID.randomUUID().toString();
      jobExecution.put(JOB_EXECUTION_ID_FIELD, id);
      PostgresClient.getInstance(vertx, tenantId).save(JOB_EXECUTIONS_TABLE_NAME, id, jobExecution.mapTo(JobExecution.class), postReply -> {
        if (postReply.failed()) {
          logger.error("Error while saving the JobExecution entity to the db", postReply.cause());
          asyncResultHandler.handle(Future.failedFuture(postReply.cause()));
        } else {
          asyncResultHandler.handle(Future.succeededFuture(jobExecution));
        }
      });
    } catch (Exception e) {
      logger.error("Error while creating new JobExecution entity", e);
      asyncResultHandler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public void updateJobExecution(String tenantId, JsonObject jobExecution, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    try {
      String id = jobExecution.getString(JOB_EXECUTION_ID_FIELD);
      Criteria criteria = constructCriteria(JOB_EXECUTION_ID_JSONB_FIELD, id);
      PostgresClient.getInstance(vertx, tenantId).update(JOB_EXECUTIONS_TABLE_NAME, jobExecution.mapTo(JobExecution.class), new Criterion(criteria), true, putReply -> {
        if (putReply.failed()) {
          logger.error("Error while updating the JobExecution's " + id + " in the db", putReply.cause());
          asyncResultHandler.handle(Future.failedFuture(putReply.cause()));
        } else if (putReply.result().getUpdated() == 0) {
          logger.debug("JobExecution " + id + " was not found in the db");
          asyncResultHandler.handle(Future.succeededFuture(null));
        } else {
          asyncResultHandler.handle(Future.succeededFuture(jobExecution));
        }
      });
    } catch (Exception e) {
      logger.error("Error while updating the JobExecution in the db", e);
      asyncResultHandler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public void getJobExecutionById(String tenantId, String jobExecutionId, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    try {
      Criteria criteria = constructCriteria(JOB_EXECUTION_ID_JSONB_FIELD, jobExecutionId);
      PostgresClient.getInstance(vertx, tenantId).get(JOB_EXECUTIONS_TABLE_NAME, JobExecution.class, new Criterion(criteria), true, false, getReply -> {
        if (getReply.failed()) {
          logger.error("Error while querying the db to get the JobExecution by id", getReply.cause());
          asyncResultHandler.handle(Future.failedFuture(getReply.cause()));
        } else {
          List<JobExecution> jobExecutionList = (List<JobExecution>) getReply.result().getResults();
          if (jobExecutionList.isEmpty()) {
            logger.debug("JobExecution with id : " + jobExecutionId + "was not found in the db");
            asyncResultHandler.handle(Future.succeededFuture(null));
          } else {
            asyncResultHandler.handle(Future.succeededFuture(JsonObject.mapFrom(jobExecutionList.get(0))));
          }
        }
      });
    } catch (Exception e) {
      logger.error("Error while getting JobExecution by id", e);
      asyncResultHandler.handle(Future.failedFuture(e));
    }
  }

  @Override
  public void deleteJobExecutionById(String tenantId, String jobExecutionId, Handler<AsyncResult<JsonObject>> asyncResultHandler) {
    try {
      Criteria criteria = constructCriteria(JOB_EXECUTION_ID_JSONB_FIELD, jobExecutionId);
      PostgresClient.getInstance(vertx, tenantId).delete(JOB_EXECUTIONS_TABLE_NAME, new Criterion(criteria), deleteReply -> {
        if (deleteReply.failed()) {
          logger.error("Error while deleting the JobExecution entity from the db", deleteReply.cause());
          asyncResultHandler.handle(Future.failedFuture(deleteReply.cause()));
        } else {
          asyncResultHandler.handle(Future.succeededFuture());
        }
      });
    } catch (Exception e) {
      logger.error("Error while deleting JobExecution by id", e);
      asyncResultHandler.handle(Future.failedFuture(e));
    }
  }

  /**
   * Builds criteria by which db result is filtered
   *
   * @param jsonbField - json key name
   * @param value      - value corresponding to the key
   * @return - Criteria object
   */
  private Criteria constructCriteria(String jsonbField, String value) {
    Criteria criteria = new Criteria();
    criteria.addField(jsonbField);
    criteria.setOperation("=");
    criteria.setValue(value);
    return criteria;
  }

  /**
   * Build CQL from request URL query
   *
   * @param query - query from URL
   * @param limit - limit of records for pagination
   * @return - CQL wrapper for building postgres request to database
   * @throws org.z3950.zing.cql.cql2pgjson.FieldException
   */
  private CQLWrapper getCQLWrapper(String query, int limit, int offset) throws org.z3950.zing.cql.cql2pgjson.FieldException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON(JOB_EXECUTIONS_TABLE_NAME + ".jsonb");
    return new CQLWrapper(cql2pgJson, query)
      .setLimit(new Limit(limit))
      .setOffset(new Offset(offset));
  }

}
