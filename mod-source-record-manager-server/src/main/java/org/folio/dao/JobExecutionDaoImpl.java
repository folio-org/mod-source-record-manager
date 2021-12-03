package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.JobExecutionMutator;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.JobExecutionDtoCollection;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Progress;
import org.folio.rest.jaxrs.model.RunBy;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.SQLConnection;
import org.folio.util.ResourceUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import javax.ws.rs.NotFoundException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static org.folio.dao.util.JobExecutionsColumns.COMPLETED_DATE_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.CURRENTLY_PROCESSED_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.ERROR_STATUS_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.FILE_NAME_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.HRID_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.ID_COLUMN;
import static org.folio.dao.util.JobExecutionsColumns.JOB_PROFILE_DATA_TYPE_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.JOB_PROFILE_ID_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.JOB_PROFILE_NAME_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.JOB_USER_FIRST_NAME_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.JOB_USER_LAST_NAME_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.PARENT_ID_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.PROFILE_SNAPSHOT_WRAPPER_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.PROGRESS_CURRENT_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.PROGRESS_TOTAL_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.SOURCE_PATH_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.STARTED_DATE_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.STATUS_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.SUBORDINATION_TYPE_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.UI_STATUS_FIELD;
import static org.folio.dao.util.JobExecutionsColumns.USER_ID_FIELD;
import static org.folio.rest.persist.PostgresClient.convertToPsqlStandard;

/**
 * Implementation for the JobExecutionDao, works with PostgresClient to access data.
 *
 * @see JobExecution
 * @see JobExecutionDao
 * @see org.folio.rest.persist.PostgresClient
 */
@Repository
public class JobExecutionDaoImpl implements JobExecutionDao {

  private static final Logger LOGGER = LogManager.getLogger();

  private static final String TABLE_NAME = "job_executions_new";
  private static final String PROGRESS_TABLE_NAME = "job_execution_progress";
  private static final String ID_FIELD = "id";
  public static final String GET_JOB_EXECUTION_HR_ID = "SELECT nextval('%s.job_execution_hr_id_sequence')";
  public static final String GET_JOBS_WITHOUT_PARENT_MULTIPLE_QUERY_PATH = "templates/db_scripts/get_job_execution_without_parent_multiple.sql";

  private static final String GET_BY_ID_SQL = "SELECT * FROM %s WHERE id = $1";

  private static final String INSERT_SQL =
    "INSERT INTO %s.%s (id, hrid, parent_job_id, subordination_type, source_path, file_name, " +
    "progress_current, progress_total, started_date, completed_date, status, ui_status, error_status, job_user_first_name, " +
    "job_user_last_name, user_id, job_profile_id, job_profile_name, job_profile_data_type, job_profile_snapshot_wrapper) " +
    "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)";

  public static final String UPDATE_SQL =
    "UPDATE %s " +
    "SET id = $1, hrid = $2, parent_job_id = $3, subordination_type = $4, source_path = $5, file_name = $6, " +
    "progress_current = $7, progress_total = $8, started_date = $9, completed_date = $10, " +
    "status = $11, ui_status = $12, error_status = $13, job_user_first_name = $14, job_user_last_name = $15, " +
    "user_id = $16, job_profile_id = $17, job_profile_name = $18, job_profile_data_type = $19, " +
    "job_profile_snapshot_wrapper = $20 " +
    "WHERE id = $1";

  private static final String GET_CHILDREN_JOBS_BY_PARENT_ID_SQL =
    "WITH cte AS (SELECT count(*) AS total_count FROM %s " +
    "WHERE parent_job_id = $1 AND subordination_type = 'CHILD') " +
    "SELECT j.*, cte.*, (p.jsonb -> 'currentlySucceeded')::int + (p.jsonb -> 'currentlyFailed')::int currently_processed " +
    "FROM %s j " +
    "LEFT JOIN %s p ON  j.id = p.jobexecutionid " +
    "LEFT JOIN cte ON true " +
    "WHERE parent_job_id = $1 AND subordination_type = 'CHILD' " +
    "LIMIT $2 OFFSET $3";

  private static final String GET_JOBS_NOT_PARENT_SQL =
    "WITH cte AS (SELECT count(*) AS total_count FROM %s " +
    "WHERE subordination_type <> 'PARENT_MULTIPLE' AND %s) " +
    "SELECT j.*, cte.*, (p.jsonb -> 'currentlySucceeded')::int + (p.jsonb -> 'currentlyFailed')::int currently_processed " +
    "FROM %s j " +
    "LEFT JOIN %s p ON  j.id = p.jobexecutionid " +
    "LEFT JOIN cte ON true " +
    "WHERE subordination_type <> 'PARENT_MULTIPLE' AND %s " +
    "ORDER BY %s %s " +
    "LIMIT $1 OFFSET $2";

  private String getJobsWithoutParentMultipleSql;

  @Autowired
  private PostgresClientFactory pgClientFactory;

  @PostConstruct
  public void init() {
    getJobsWithoutParentMultipleSql = ResourceUtil.asString(GET_JOBS_WITHOUT_PARENT_MULTIPLE_QUERY_PATH);
  }

  @Override
  public Future<JobExecutionDtoCollection> getJobExecutionsWithoutParentMultiple(JobExecutionFilter filter, int offset, int limit, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String filterCriteria = filter.buildWhereClause();
      String sortBy = "completed_date";
      String jobTable = formatFullTableName(tenantId, TABLE_NAME);
      String progressTable = formatFullTableName(tenantId, PROGRESS_TABLE_NAME);
      String query2 = format(GET_JOBS_NOT_PARENT_SQL, jobTable, filterCriteria, jobTable, progressTable, filterCriteria,  sortBy, "asc");
      pgClientFactory.createInstance(tenantId).select(query2, Tuple.of(limit, offset), promise);
    } catch (Exception e) {
      LOGGER.error("Error while getting Logs", e);
      promise.fail(e);
    }
    return promise.future().map(this::mapToJobExecutionDtoCollection);
  }

  @Override
  public Future<JobExecutionDtoCollection> getChildrenJobExecutionsByParentId(String parentId, String query, int offset, int limit, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String jobTable = formatFullTableName(tenantId, TABLE_NAME);
      String progressTable = formatFullTableName(tenantId, PROGRESS_TABLE_NAME);
      String sql = format(GET_CHILDREN_JOBS_BY_PARENT_ID_SQL, jobTable, jobTable, progressTable);
      Tuple queryParams = Tuple.of(UUID.fromString(parentId), limit, offset);
      pgClientFactory.createInstance(tenantId).select(sql, queryParams, promise);
    } catch (Exception e) {
      LOGGER.error("Error getting jobExecutions by parent id", e);
      promise.fail(e);
    }
    return promise.future().map(this::mapToJobExecutionDtoCollection);
  }

  @Override
  public Future<Optional<JobExecution>> getJobExecutionById(String id, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String jobTable = formatFullTableName(tenantId, TABLE_NAME);
      String query = format(GET_BY_ID_SQL, jobTable);
      pgClientFactory.createInstance(tenantId).select(query, Tuple.of(UUID.fromString(id)), promise);
    } catch (Exception e) {
      LOGGER.error("Error getting jobExecution by id", e);
      promise.fail(e);
    }
    return promise.future().map(rowSet -> rowSet.rowCount() == 0 ? Optional.empty()
      : Optional.of(mapRowToJobExecution(rowSet.iterator().next())));
  }

  @Override
  public Future<String> save(JobExecution jobExecution, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String preparedQuery = String.format(GET_JOB_EXECUTION_HR_ID, PostgresClient.convertToPsqlStandard(tenantId));
    pgClientFactory.createInstance(tenantId).select(preparedQuery, getHrIdAr -> {
      if (getHrIdAr.succeeded() && getHrIdAr.result().iterator().hasNext()) {
        jobExecution.setHrId(getHrIdAr.result().iterator().next().getInteger(0));
        String query = format(INSERT_SQL, convertToPsqlStandard(tenantId), TABLE_NAME);
        pgClientFactory.createInstance(tenantId).execute(query, mapToTuple(jobExecution), promise);
      } else {
        promise.fail(getHrIdAr.cause());
      }
    });
    return promise.future().map(jobExecution.getId());
  }

  @Override
  public Future<JobExecution> updateJobExecution(JobExecution jobExecution, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String errorMessage = String.format("JobExecution with id '%s' was not found", jobExecution.getId());
    try {
      String preparedQuery = format(UPDATE_SQL, formatFullTableName(tenantId, TABLE_NAME));
      Tuple queryParams = mapToTuple(jobExecution);
      pgClientFactory.createInstance(tenantId).execute(preparedQuery, queryParams, promise);
    } catch (Exception e) {
      LOGGER.error("Error updating jobExecution", e);
      promise.fail(e);
    }
    return promise.future().compose(rowSet -> rowSet.rowCount() != 1
      ? Future.failedFuture(new NotFoundException(errorMessage)) : Future.succeededFuture(jobExecution));
  }

  @Override
  public Future<JobExecution> updateBlocking(String jobExecutionId, JobExecutionMutator mutator, String tenantId) {
    Promise<JobExecution> promise = Promise.promise();
    String rollbackMessage = "Rollback transaction. Error during jobExecution update. jobExecutionId" + jobExecutionId;
    Promise<SQLConnection> connection = Promise.promise();
    Promise<JobExecution> jobExecutionPromise = Promise.promise();
    Future.succeededFuture()
      .compose(v -> {
        pgClientFactory.createInstance(tenantId).startTx(connection);
        return connection.future();
      }).compose(v -> {
        String selectForUpdate = format("SELECT * FROM %s WHERE id = $1 LIMIT 1 FOR UPDATE", formatFullTableName(tenantId, TABLE_NAME));
        Promise<RowSet<Row>> selectResult = Promise.promise();
        pgClientFactory.createInstance(tenantId).execute(connection.future(), selectForUpdate, Tuple.of(jobExecutionId), selectResult);
        return selectResult.future();
      }).compose(rowSet -> {
        if (rowSet.rowCount() != 1) {
          throw new NotFoundException(rollbackMessage);
        }
        return mutator.mutate(mapRowToJobExecution(rowSet.iterator().next())).onComplete(jobExecutionPromise);
      }).compose(jobExecution -> {
        Promise<RowSet<Row>> updateHandler = Promise.promise();
        String preparedQuery = format(UPDATE_SQL, formatFullTableName(tenantId, TABLE_NAME));
        Tuple queryParams = mapToTuple(jobExecution);
        pgClientFactory.createInstance(tenantId).execute(connection.future(), preparedQuery, queryParams, updateHandler);
        return updateHandler.future();
      }).compose(updateHandler -> {
        Promise<Void> endTxFuture = Promise.promise();
        pgClientFactory.createInstance(tenantId).endTx(connection.future(), endTxFuture);
        return endTxFuture.future();
      }).onComplete(ar -> {
        if (ar.failed()) {
          pgClientFactory.createInstance(tenantId).rollbackTx(connection.future(), rollback -> promise.fail(ar.cause()));
          return;
        }
        promise.complete(jobExecutionPromise.future().result());
      });
    return promise.future();
  }

  @Override
  public Future<Boolean> deleteJobExecutionById(String jobExecutionId, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    pgClientFactory.createInstance(tenantId).delete(TABLE_NAME, jobExecutionId, promise);
    return promise.future().map(updateResult -> updateResult.rowCount() == 1);
  }

  private Tuple mapToTuple(JobExecution jobExecution) {
    return Tuple.of(UUID.fromString(jobExecution.getId()),
      jobExecution.getHrId(),
      UUID.fromString(jobExecution.getParentJobId()),
      jobExecution.getSubordinationType().toString(),
      jobExecution.getSourcePath(),
      jobExecution.getFileName(),
      jobExecution.getProgress().getCurrent(),
      jobExecution.getProgress().getTotal(),
      jobExecution.getStartedDate().toInstant().atOffset(ZoneOffset.UTC),
      jobExecution.getCompletedDate() != null ? jobExecution.getCompletedDate().toInstant().atOffset(ZoneOffset.UTC) : null,
      jobExecution.getStatus().toString(),
      jobExecution.getUiStatus().toString(),
      jobExecution.getErrorStatus() != null ? jobExecution.getErrorStatus().toString() : null,
      jobExecution.getRunBy().getFirstName(),
      jobExecution.getRunBy().getLastName(),
      UUID.fromString(jobExecution.getUserId()),
      jobExecution.getJobProfileInfo() != null ? UUID.fromString(jobExecution.getJobProfileInfo().getId()) : null,
      jobExecution.getJobProfileInfo() != null ? jobExecution.getJobProfileInfo().getName() : null,
      nonNull(jobExecution.getJobProfileInfo()) && nonNull(jobExecution.getJobProfileInfo().getDataType())
        ? jobExecution.getJobProfileInfo().getDataType().toString() : null,
      jobExecution.getJobProfileSnapshotWrapper() != null
        ? JsonObject.mapFrom(jobExecution.getJobProfileSnapshotWrapper()) : null);
  }

  private JobExecutionDtoCollection mapToJobExecutionDtoCollection(RowSet<Row> rowSet) {
    JobExecutionDtoCollection jobCollection = new JobExecutionDtoCollection().withTotalRecords(0);
    for (Row row : rowSet) {
      jobCollection.getJobExecutions().add(mapRowToJobExecutionDto(row));
      jobCollection.setTotalRecords(row.getInteger("total_count"));
    }
    return jobCollection;
  }

  private JobExecution mapRowToJobExecution(Row row) {
    return new JobExecution()
      .withId(row.getValue(ID_COLUMN).toString())
      .withHrId(row.getInteger(HRID_FIELD))
      .withParentJobId(row.getValue(PARENT_ID_FIELD).toString())
      .withSubordinationType(JobExecution.SubordinationType.fromValue(row.getString(SUBORDINATION_TYPE_FIELD)))
      .withSourcePath(row.getString(SOURCE_PATH_FIELD))
      .withFileName(row.getString(FILE_NAME_FIELD))
      .withStartedDate(Date.from(row.getLocalDateTime(STARTED_DATE_FIELD).toInstant(ZoneOffset.UTC)))
      .withCompletedDate(row.getLocalDateTime(COMPLETED_DATE_FIELD) != null
        ? Date.from(LocalDateTime.parse(row.getLocalDateTime(COMPLETED_DATE_FIELD).toString()).toInstant(ZoneOffset.UTC)) : null)
      .withStatus(row.get(JobExecution.Status.class, STATUS_FIELD))
      .withUiStatus(row.get(JobExecution.UiStatus.class, UI_STATUS_FIELD))
      .withErrorStatus(row.getString(ERROR_STATUS_FIELD) != null ? row.get(JobExecution.ErrorStatus.class, ERROR_STATUS_FIELD) : null)
      .withRunBy(new RunBy()
        .withFirstName(row.getString(JOB_USER_FIRST_NAME_FIELD))
        .withLastName(row.getString(JOB_USER_LAST_NAME_FIELD)))
      .withUserId(row.getValue(USER_ID_FIELD).toString())
      .withProgress(new Progress()
        .withJobExecutionId(row.getValue(ID_COLUMN).toString())
        .withCurrent(row.getInteger(PROGRESS_CURRENT_FIELD))
        .withTotal(row.getInteger(PROGRESS_TOTAL_FIELD)))
      .withJobProfileInfo(mapRowToJobProfileInfo(row))
      .withJobProfileSnapshotWrapper(row.getJsonObject(PROFILE_SNAPSHOT_WRAPPER_FIELD) != null
        ? row.getJsonObject(PROFILE_SNAPSHOT_WRAPPER_FIELD).mapTo(ProfileSnapshotWrapper.class) : null);
  }

  private JobExecutionDto mapRowToJobExecutionDto(Row row) {
    return new JobExecutionDto()
      .withId(row.getValue(ID_COLUMN).toString())
      .withHrId(row.getInteger(HRID_FIELD))
      .withParentJobId(row.getValue(PARENT_ID_FIELD).toString())
      .withSubordinationType(JobExecutionDto.SubordinationType.fromValue(row.getString(SUBORDINATION_TYPE_FIELD)))
      .withSourcePath(row.getString(SOURCE_PATH_FIELD))
      .withFileName(row.getString(FILE_NAME_FIELD))
      .withStartedDate(Date.from(row.getLocalDateTime(STARTED_DATE_FIELD).toInstant(ZoneOffset.UTC)))
      .withCompletedDate(row.getLocalDateTime(COMPLETED_DATE_FIELD) != null
        ? Date.from(LocalDateTime.parse(row.getLocalDateTime(COMPLETED_DATE_FIELD).toString()).toInstant(ZoneOffset.UTC)) : null)
      .withStatus(row.get(JobExecutionDto.Status.class, STATUS_FIELD))
      .withUiStatus(row.get(JobExecutionDto.UiStatus.class, UI_STATUS_FIELD))
      .withErrorStatus(row.getString(ERROR_STATUS_FIELD) != null
        ? row.get(JobExecutionDto.ErrorStatus.class, ERROR_STATUS_FIELD) : null)
      .withRunBy(new RunBy()
        .withFirstName(row.getString(JOB_USER_FIRST_NAME_FIELD))
        .withLastName(row.getString(JOB_USER_LAST_NAME_FIELD)))
      .withUserId(row.getValue(USER_ID_FIELD).toString())
      .withProgress(mapRowToProgress(row))
      .withJobProfileInfo(mapRowToJobProfileInfo(row));
  }

  private Progress mapRowToProgress(Row row) {
    Integer processedCount = row.getInteger(CURRENTLY_PROCESSED_FIELD);
    if (processedCount == null) {
      processedCount = row.getInteger(PROGRESS_CURRENT_FIELD);
    }

    return new Progress()
      .withJobExecutionId(row.getValue(ID_COLUMN).toString())
      .withCurrent(processedCount)
      .withTotal(row.getInteger(PROGRESS_TOTAL_FIELD));
  }

  private JobProfileInfo mapRowToJobProfileInfo(Row row) {
    UUID profileId = row.getUUID(JOB_PROFILE_ID_FIELD);
    if (profileId != null) {
      return new JobProfileInfo()
        .withId(profileId.toString())
        .withName(row.getString(JOB_PROFILE_NAME_FIELD))
        .withDataType(row.getString(JOB_PROFILE_DATA_TYPE_FIELD) == null
          ? null : JobProfileInfo.DataType.fromValue(row.getString(JOB_PROFILE_DATA_TYPE_FIELD)));
    }
    return null;
  }

  private String formatFullTableName(String tenantId, String table) {
    return format("%s.%s", convertToPsqlStandard(tenantId), table);
  }

}
