package org.folio.dao;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import org.folio.cql2pgjson.exception.FieldException;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.JobExecutionProgress;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.ws.rs.NotFoundException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.UnaryOperator;

import static org.folio.dataimport.util.DaoUtil.constructCriteria;
import static org.folio.dataimport.util.DaoUtil.getCQLWrapper;

@Repository
public class JobExecutionProgressDaoImpl implements JobExecutionProgressDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutionProgressDaoImpl.class);

  private static final String TABLE_NAME = "job_execution_progress";
  public static final String JOB_EXECUTION_ID_FIELD = "'jobExecutionId'";

  @Autowired
  private PostgresClientFactory pgClientFactory;

  @Override
  public Future<Optional<JobExecutionProgress>> getByJobExecutionId(String jobExecutionId, String tenantId) {
    Promise<Results<JobExecutionProgress>> promise = Promise.promise();
    Criteria jobIdCrit = constructCriteria(JOB_EXECUTION_ID_FIELD, jobExecutionId);
    pgClientFactory.createInstance(tenantId).get(TABLE_NAME, JobExecutionProgress.class, new Criterion(jobIdCrit), true, false, promise);
    return promise.future()
      .map(Results::getResults)
      .map(progressList -> progressList.isEmpty() ? Optional.empty() : Optional.of(progressList.get(0)));
  }

  @Override
  public Future<JobExecutionProgress> initializeJobExecutionProgress(String jobExecutionId, Integer totalRecords, String tenantId) {
    Promise<JobExecutionProgress> promise = Promise.promise();
    Promise<SQLConnection> tx = Promise.promise();
    Criteria jobIdCrit = constructCriteria(JOB_EXECUTION_ID_FIELD, jobExecutionId);
    PostgresClient client = pgClientFactory.createInstance(tenantId);
    Future.succeededFuture()
      .compose(v -> {
        client.startTx(tx);
        return tx.future();
      })
      .compose(sqlConnection -> {
        StringBuilder selectProgressQuery = new StringBuilder("SELECT jsonb FROM ")
          .append(PostgresClient.convertToPsqlStandard(tenantId))
          .append(".")
          .append(TABLE_NAME)
          .append(" WHERE jsonb ->> 'jobExecutionId' = '")
          .append(jobExecutionId)
          .append("' LIMIT 1 FOR UPDATE;");
        Promise<UpdateResult> selectResult = Promise.promise();
        client.execute(tx.future(), selectProgressQuery.toString(), selectResult);
        return selectResult.future();
      })
      .compose(selectResult -> {
        Promise<Results<JobExecutionProgress>> getProgressPromise = Promise.promise();
        client.get(tx.future(), TABLE_NAME, JobExecutionProgress.class, new Criterion(jobIdCrit), false, true, getProgressPromise);
        return getProgressPromise.future();
      })
      .compose(selectResult -> {
        JobExecutionProgress progress = new JobExecutionProgress()
          .withJobExecutionId(jobExecutionId)
          .withTotal(totalRecords)
          .withId(UUID.randomUUID().toString());
        return selectResult.getResults().isEmpty() ? save(progress, tenantId).map(progress) : Future.succeededFuture(selectResult.getResults().get(0));
      })
      .setHandler(updateAr -> {
        if (updateAr.succeeded()) {
          client.endTx(tx.future(), endTx -> promise.complete(updateAr.result()));
        } else {
          LOGGER.error("Fail to initialize JobExecutionProgress for job with id:" + jobExecutionId, updateAr.cause());
          client.rollbackTx(tx.future(), r -> promise.fail(updateAr.cause()));
        }
      });
    return promise.future();
  }

  @Override
  public Future<String> save(JobExecutionProgress progress, String tenantId) {
    Promise<String> promise = Promise.promise();
    progress.withId(UUID.randomUUID().toString());
    pgClientFactory.createInstance(tenantId).save(TABLE_NAME, progress, true, promise);
    return promise.future();
  }

  @Override
  public Future<JobExecutionProgress> updateByJobExecutionId(String jobExecutionId, UnaryOperator<JobExecutionProgress> progressMutator, String tenantId) {
    String rollbackMessage = String.format("Rollback transaction. Failed to update jobExecutionProgress for jobExecution with id '%s", jobExecutionId);
    Promise<JobExecutionProgress> promise = Promise.promise();
    Promise<SQLConnection> tx = Promise.promise();
    PostgresClient pgClient = pgClientFactory.createInstance(tenantId);
    Criteria jobIdCrit = constructCriteria(JOB_EXECUTION_ID_FIELD, jobExecutionId);

    Future.succeededFuture()
      .compose(v -> {
        pgClient.startTx(tx);
        return tx.future();
      })
      .compose(sqlConnection -> {
        StringBuilder selectProgressQuery = new StringBuilder("SELECT jsonb FROM ")
          .append(PostgresClient.convertToPsqlStandard(tenantId))
          .append(".")
          .append(TABLE_NAME)
          .append(" WHERE jsonb ->> 'jobExecutionId' = '")
          .append(jobExecutionId)
          .append("' LIMIT 1 FOR UPDATE;");
        Promise<UpdateResult> selectResult = Promise.promise();
        pgClient.execute(tx.future(), selectProgressQuery.toString(), selectResult);
        return selectResult.future();
      })
      .compose(selectResult -> {
        Promise<Results<JobExecutionProgress>> getProgressPromise = Promise.promise();
        pgClient.get(tx.future(), TABLE_NAME, JobExecutionProgress.class, new Criterion(jobIdCrit), false, true, getProgressPromise);
        return getProgressPromise.future();
      })
      .map(progressResults -> {
        if (progressResults.getResults().size() != 1) {
          throw new NotFoundException(rollbackMessage);
        }
        return progressMutator.apply(progressResults.getResults().get(0));
      })
      .compose(mutatedProgress -> updateProgressByJobExecutionId(tx.future(), mutatedProgress, tenantId))
      .setHandler(updateAr -> {
        if (updateAr.succeeded()) {
          pgClient.endTx(tx.future(), endTx -> promise.complete(updateAr.result()));
        } else {
          LOGGER.error(rollbackMessage, updateAr.cause());
          pgClient.rollbackTx(tx.future(), r -> promise.fail(updateAr.cause()));
        }
      });
    return promise.future();
  }

  private Future<JobExecutionProgress> updateProgressByJobExecutionId(AsyncResult<SQLConnection> tx, JobExecutionProgress progress, String tenantId) {
    Promise<UpdateResult> promise = Promise.promise();
    try {
      String query = "jobExecutionId==" + progress.getJobExecutionId();
      CQLWrapper cqlWrapper = getCQLWrapper(TABLE_NAME, query);
      pgClientFactory.createInstance(tenantId).update(tx, TABLE_NAME, progress, cqlWrapper, true, promise);
    } catch (FieldException e) {
      LOGGER.error("Failed to update jobExecutionProgress for jobExecution with id {}", e, progress.getJobExecutionId());
      promise.fail(e);
    }
    return promise.future().map(progress);
  }
}
