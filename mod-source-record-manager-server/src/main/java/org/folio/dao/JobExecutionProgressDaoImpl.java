package org.folio.dao;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

import java.util.Optional;
import java.util.function.UnaryOperator;
import javax.ws.rs.NotFoundException;

import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.JobExecutionProgress;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.SQLConnection;
import org.folio.rest.tools.utils.ValidationHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import static org.folio.rest.persist.PostgresClient.convertToPsqlStandard;

@Repository
public class JobExecutionProgressDaoImpl implements JobExecutionProgressDao {

  private static final Logger LOGGER = LogManager.getLogger();

  public static final String JOB_EXECUTION_ID_FIELD = "job_execution_id";
  public static final String TOTAL_RECORDS_COUNT = "total_records_count";
  public static final String PROCESSED_RECORDS_COUNT = "processed_records_count";
  public static final String ERROR_RECORDS_COUNT = "error_records_count";

  private static final String TABLE_NAME = "job_execution_progress";
  private static final String INSERT_SQL = "INSERT INTO %s.%s (job_execution_id, total_records_count, processed_records_count, error_records_count) VALUES ($1, $2, $3, $4)";
  private static final String UPDATE_SQL = "UPDATE %s.%s SET job_execution_id = $1, total_records_count = $2, processed_records_count = $3, error_records_count = $4 WHERE job_execution_id = $1";
  private static final String SELECT_BY_JOB_EXECUTION_ID_QUERY = "SELECT * FROM %s.%s WHERE job_execution_id = $1";
  private static final String SELECT_FROM_JOB_EXECUTION_PROGRESS = "SELECT * FROM %s.%s WHERE job_execution_id = $1 LIMIT 1 FOR UPDATE";
  private static final String ROLLBACK_MESSAGE = "Rollback transaction. Failed to update jobExecutionProgress with job_execution_id: %s";
  private static final String FAILED_INITIALIZATION_MESSAGE = "Fail to initialize JobExecutionProgress for job with job_execution_id: {}";

  @Autowired
  private PostgresClientFactory pgClientFactory;

  @Override
  public Future<Optional<JobExecutionProgress>> getByJobExecutionId(String jobExecutionId, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String query = buildSelectByJobExecutionIdQuery(tenantId);
    Tuple queryParams = Tuple.of(jobExecutionId);
    pgClientFactory.createInstance(tenantId).execute(query, queryParams, promise);
    return promise.future().map(this::mapResultSetToOptionalJobExecutionProgress);
  }

  @Override
  public Future<JobExecutionProgress> initializeJobExecutionProgress(String jobExecutionId, Integer totalRecords, String tenantId) {
    Promise<JobExecutionProgress> promise = Promise.promise();
    Promise<SQLConnection> tx = Promise.promise();
    PostgresClient pgClient = pgClientFactory.createInstance(tenantId);

    getSelectResult(tx, pgClient, jobExecutionId, tenantId)
      .compose(selectResult -> {
        JobExecutionProgress progress = new JobExecutionProgress()
          .withJobExecutionId(jobExecutionId)
          .withTotal(totalRecords);

        Optional<JobExecutionProgress> optionalJobExecutionProgress = mapResultSetToOptionalJobExecutionProgress(selectResult);
        return optionalJobExecutionProgress.isEmpty() ? save(progress, tenantId).map(progress) : Future.succeededFuture(optionalJobExecutionProgress.get());
      })
      .onComplete(saveAr -> {
        if (saveAr.succeeded()) {
          pgClient.endTx(tx.future(), endTx -> promise.complete(saveAr.result()));
        } else {
          if (ValidationHelper.isDuplicate(saveAr.cause().getMessage())) {
            pgClient.rollbackTx(tx.future(), r -> promise.complete());
            return;
          }
          LOGGER.error(FAILED_INITIALIZATION_MESSAGE, jobExecutionId, saveAr.cause());
          pgClient.rollbackTx(tx.future(), r -> promise.fail(saveAr.cause()));
        }
      });
    return promise.future();
  }

  @Override
  public Future<JobExecutionProgress> updateByJobExecutionId(String jobExecutionId, UnaryOperator<JobExecutionProgress> progressMutator, String tenantId) {
    String rollbackMessage = String.format(ROLLBACK_MESSAGE, jobExecutionId);
    Promise<JobExecutionProgress> promise = Promise.promise();
    Promise<SQLConnection> tx = Promise.promise();
    PostgresClient pgClient = pgClientFactory.createInstance(tenantId);

    getSelectResult(tx, pgClient, jobExecutionId, tenantId)
      .map(progressResults -> {
        Optional<JobExecutionProgress> optionalJobExecutionProgress = mapResultSetToOptionalJobExecutionProgress(progressResults);
        if (optionalJobExecutionProgress.isEmpty()) {
          throw new NotFoundException(rollbackMessage);
        }
        return progressMutator.apply(optionalJobExecutionProgress.get());
      })
      .compose(mutatedProgress -> updateProgressByJobExecutionId(tx.future(), mutatedProgress, tenantId))
      .onComplete(updateAr -> {
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
    Promise<RowSet<Row>> promise = Promise.promise();
    String query = String.format(UPDATE_SQL, convertToPsqlStandard(tenantId), TABLE_NAME);
    Tuple queryParams = Tuple.of(progress.getJobExecutionId(), progress.getTotal(), progress.getCurrentlySucceeded(), progress.getCurrentlyFailed());
    pgClientFactory.createInstance(tenantId).execute(tx, query, queryParams, promise);
    return promise.future().map(progress)
      .onFailure(e -> LOGGER.error("Failed to update jobExecutionProgress with job_execution_id: {}", progress.getJobExecutionId(), e));
  }

  @Override
  public Future<RowSet<Row>> save(JobExecutionProgress progress, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String query = String.format(INSERT_SQL, convertToPsqlStandard(tenantId), TABLE_NAME);
    Tuple queryParams = Tuple.of(progress.getJobExecutionId(), progress.getTotal(), progress.getCurrentlySucceeded(), progress.getCurrentlyFailed());
    pgClientFactory.createInstance(tenantId).execute(query, queryParams, promise);
    return promise.future();
  }

  private Future<RowSet<Row>> getSelectResult(Promise<SQLConnection> tx, PostgresClient pgClient, String jobExecutionId, String tenantId) {
    Tuple queryParams = Tuple.of(jobExecutionId);

    return Future.succeededFuture()
      .compose(v -> {
        pgClient.startTx(tx);
        return tx.future();
      })
      .compose(sqlConnection -> {
        String selectProgressQuery = buildSelectProgressQuery(tenantId);
        Promise<RowSet<Row>> selectResult = Promise.promise();
        pgClient.execute(tx.future(), selectProgressQuery, queryParams, selectResult);
        return selectResult.future();
      })
      .compose(selectResult -> {
        Promise<RowSet<Row>> getProgressPromise = Promise.promise();
        String query = buildSelectByJobExecutionIdQuery(tenantId);
        pgClient.execute(tx.future(), query, queryParams, getProgressPromise);
        return getProgressPromise.future();
      });
  }

  private Optional<JobExecutionProgress> mapResultSetToOptionalJobExecutionProgress(RowSet<Row> resultSet) {
    RowIterator<Row> iterator = resultSet.iterator();
    return iterator.hasNext() ? Optional.of(mapRowToJobExecutionProgress(iterator.next())) : Optional.empty();
  }

  private JobExecutionProgress mapRowToJobExecutionProgress(Row row) {
    return new JobExecutionProgress()
      .withJobExecutionId(row.getValue(JOB_EXECUTION_ID_FIELD).toString())
      .withTotal(row.getInteger(TOTAL_RECORDS_COUNT))
      .withCurrentlySucceeded(row.getInteger(PROCESSED_RECORDS_COUNT))
      .withCurrentlyFailed(row.getInteger(ERROR_RECORDS_COUNT));
  }

  private String buildSelectProgressQuery(String tenantId) {
    return String.format(SELECT_FROM_JOB_EXECUTION_PROGRESS, PostgresClient.convertToPsqlStandard(tenantId), TABLE_NAME);
  }

  private String buildSelectByJobExecutionIdQuery(String tenantId) {
    return String.format(SELECT_BY_JOB_EXECUTION_ID_QUERY, convertToPsqlStandard(tenantId), TABLE_NAME);
  }
}
