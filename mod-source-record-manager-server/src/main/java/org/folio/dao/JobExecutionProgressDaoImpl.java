package org.folio.dao;

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
import org.folio.rest.persist.Conn;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.ValidationHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import static java.lang.String.format;
import static org.folio.rest.persist.PostgresClient.convertToPsqlStandard;

@Repository
public class JobExecutionProgressDaoImpl implements JobExecutionProgressDao {

  private static final Logger LOGGER = LogManager.getLogger();

  public static final String JOB_EXECUTION_ID_FIELD = "job_execution_id";
  public static final String TOTAL_RECORDS_COUNT = "total_records_count";
  public static final String SUCCEEDED_RECORDS_COUNT = "succeeded_records_count";
  public static final String ERROR_RECORDS_COUNT = "error_records_count";

  private static final String TABLE_NAME = "job_execution_progress";
  private static final String INSERT_SQL = "INSERT INTO %s.%s (job_execution_id, total_records_count, succeeded_records_count, error_records_count) VALUES ($1, $2, $3, $4)";
  private static final String UPDATE_SQL = "UPDATE %s.%s SET job_execution_id = $1, total_records_count = $2, succeeded_records_count = $3, error_records_count = $4 WHERE job_execution_id = $1";
  private static final String UPDATE_DELTA_SQL = "UPDATE %s.%s SET " +
    "succeeded_records_count = succeeded_records_count + $2, " +
    "error_records_count = error_records_count + $3 " +
    "WHERE job_execution_id = $1 " +
    "Returning *";

  private static final String SELECT_BY_JOB_EXECUTION_ID = "SELECT * FROM %s.%s WHERE job_execution_id = $1";
  private static final String SELECT_BY_JOB_EXECUTION_ID_FOR_UPDATE = "SELECT * FROM %s.%s WHERE job_execution_id = $1 LIMIT 1 FOR UPDATE";
  private static final String ROLLBACK_MESSAGE = "Rollback transaction. Failed to update jobExecutionProgress with job_execution_id: %s";
  private static final String FAILED_INITIALIZATION_MESSAGE = "Fail to initialize JobExecutionProgress for job with job_execution_id: {}";

  @Autowired
  private PostgresClientFactory pgClientFactory;

  @Override
  public Future<Optional<JobExecutionProgress>> getByJobExecutionId(String jobExecutionId, String tenantId) {
    String query = buildSelectByJobExecutionIdQuery(tenantId);
    Tuple queryParams = Tuple.of(jobExecutionId);
    return pgClientFactory.createInstance(tenantId).execute(query, queryParams)
      .map(this::mapResultSetToOptionalJobExecutionProgress);
  }

  @Override
  public Future<JobExecutionProgress> initializeJobExecutionProgress(Conn connection, String jobExecutionId, Integer totalRecords, String tenantId) {
    LOGGER.debug("initializeJobExecutionProgress:: jobExecutionId {}, totalRecords {}, tenantId {}", jobExecutionId, totalRecords, tenantId);
    Promise<JobExecutionProgress> promise = Promise.promise();

    getSelectResult(connection, jobExecutionId, tenantId)
      .compose(selectResult -> {
        JobExecutionProgress progress = new JobExecutionProgress()
          .withJobExecutionId(jobExecutionId)
          .withTotal(totalRecords);

        Optional<JobExecutionProgress> optionalJobExecutionProgress = mapResultSetToOptionalJobExecutionProgress(selectResult);
        return optionalJobExecutionProgress.isEmpty() ? save(progress, tenantId).map(progress) : Future.succeededFuture(optionalJobExecutionProgress.get());
      })
      .onComplete(saveAr -> {
        if (saveAr.succeeded()) {
          promise.complete(saveAr.result());
        } else {
          if (ValidationHelper.isDuplicate(saveAr.cause().getMessage())) {
            promise.complete();
            return;
          }
          LOGGER.warn(FAILED_INITIALIZATION_MESSAGE, jobExecutionId, saveAr.cause());
          promise.fail(saveAr.cause());
        }
      });
    return promise.future();
  }

  @Override
  public Future<JobExecutionProgress> updateByJobExecutionId(String jobExecutionId, UnaryOperator<JobExecutionProgress> progressMutator, String tenantId) {
    String rollbackMessage = String.format(ROLLBACK_MESSAGE, jobExecutionId);

    return pgClientFactory.createInstance(tenantId).withTrans(connection ->
      getSelectResult(connection, jobExecutionId, tenantId)
        .map(progressResults -> {
          Optional<JobExecutionProgress> optionalJobExecutionProgress = mapResultSetToOptionalJobExecutionProgress(progressResults);
          if (optionalJobExecutionProgress.isEmpty()) {
            throw new NotFoundException(rollbackMessage);
          }
          return progressMutator.apply(optionalJobExecutionProgress.get());
        })
        .compose(mutatedProgress -> updateProgressByJobExecutionId(connection, mutatedProgress, tenantId))
    );
  }

  @Override
  public Future<JobExecutionProgress> updateCompletionCounts(String jobExecutionId, int successCountDelta, int errorCountDelta, String tenantId) {
    try {
      String preparedQuery = format(UPDATE_DELTA_SQL, convertToPsqlStandard(tenantId), TABLE_NAME);
      Tuple queryParams = Tuple.of(jobExecutionId, successCountDelta, errorCountDelta);
      return pgClientFactory.createInstance(tenantId).execute(preparedQuery, queryParams)
        .compose(rowSet -> {
          if (rowSet.rowCount() != 1) {
            String errorMessage = String.format("Single result was not returned " +
              "when JobExecutionProgress with id '%s' was updated", jobExecutionId);
            return Future.failedFuture(new NotFoundException(errorMessage));
          }
          return Future.succeededFuture(mapRowToJobExecutionProgress(rowSet.iterator().next()));
        });
    } catch (Exception e) {
      LOGGER.warn("updateCompletionCounts:: Error updating jobExecutionProgress", e);
      return Future.failedFuture(e);
    }
  }

  private Future<JobExecutionProgress> updateProgressByJobExecutionId(Conn connection, JobExecutionProgress progress, String tenantId) {
    String query = String.format(UPDATE_SQL, convertToPsqlStandard(tenantId), TABLE_NAME);
    Tuple queryParams = Tuple.of(progress.getJobExecutionId(), progress.getTotal(), progress.getCurrentlySucceeded(), progress.getCurrentlyFailed());
    return connection.execute(query, queryParams).map(progress);
  }

  @Override
  public Future<RowSet<Row>> save(JobExecutionProgress progress, String tenantId) {
    String query = String.format(INSERT_SQL, convertToPsqlStandard(tenantId), TABLE_NAME);
    Tuple queryParams = Tuple.of(progress.getJobExecutionId(), progress.getTotal(), progress.getCurrentlySucceeded(), progress.getCurrentlyFailed());
    return pgClientFactory.createInstance(tenantId).execute(query, queryParams);
  }

  private Future<RowSet<Row>> getSelectResult(Conn connection, String jobExecutionId, String tenantId) {
    Tuple queryParams = Tuple.of(jobExecutionId);

    return Future.succeededFuture()
      .compose(v -> {
        String selectProgressQuery = buildSelectProgressQuery(tenantId);
        return connection.execute(selectProgressQuery, queryParams);
      })
      .compose(selectResult -> {
        String query = buildSelectByJobExecutionIdQuery(tenantId);
        return connection.execute(query, queryParams);
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
      .withCurrentlySucceeded(row.getInteger(SUCCEEDED_RECORDS_COUNT))
      .withCurrentlyFailed(row.getInteger(ERROR_RECORDS_COUNT));
  }

  private String buildSelectProgressQuery(String tenantId) {
    return String.format(SELECT_BY_JOB_EXECUTION_ID_FOR_UPDATE, PostgresClient.convertToPsqlStandard(tenantId), TABLE_NAME);
  }

  private String buildSelectByJobExecutionIdQuery(String tenantId) {
    return String.format(SELECT_BY_JOB_EXECUTION_ID, convertToPsqlStandard(tenantId), TABLE_NAME);
  }
}
