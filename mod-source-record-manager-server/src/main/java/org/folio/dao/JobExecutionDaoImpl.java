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
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.ws.rs.NotFoundException;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static org.folio.dao.util.JobExecutionDBConstants.COMPLETED_DATE_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.CURRENTLY_PROCESSED_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.ERROR_STATUS_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.FILE_NAME_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.GET_BY_ID_SQL;
import static org.folio.dao.util.JobExecutionDBConstants.GET_CHILDREN_JOBS_BY_PARENT_ID_SQL;
import static org.folio.dao.util.JobExecutionDBConstants.GET_JOBS_NOT_PARENT_SQL;
import static org.folio.dao.util.JobExecutionDBConstants.HRID_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.ID_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.INSERT_SQL;
import static org.folio.dao.util.JobExecutionDBConstants.JOB_PROFILE_DATA_TYPE_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.JOB_PROFILE_ID_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.JOB_PROFILE_NAME_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.JOB_USER_FIRST_NAME_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.JOB_USER_LAST_NAME_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.PARENT_ID_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.PROFILE_SNAPSHOT_WRAPPER_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.PROGRESS_CURRENT_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.PROGRESS_TOTAL_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.SOURCE_PATH_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.STARTED_DATE_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.STATUS_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.SUBORDINATION_TYPE_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.TOTAL_COUNT_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.TOTAL_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.UI_STATUS_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.UPDATE_SQL;
import static org.folio.dao.util.JobExecutionDBConstants.USER_ID_FIELD;
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

  private static final String TABLE_NAME = "job_execution";
  private static final String PROGRESS_TABLE_NAME = "job_execution_progress";
  public static final String GET_JOB_EXECUTION_HR_ID = "SELECT nextval('%s.job_execution_hr_id_sequence')";

  @Autowired
  private PostgresClientFactory pgClientFactory;

  @Override
  public Future<JobExecutionDtoCollection> getJobExecutionsWithoutParentMultiple(JobExecutionFilter filter, String sortBy, String order, int offset, int limit, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String filterCriteria = filter.buildCriteria();
      String jobTable = formatFullTableName(tenantId, TABLE_NAME);
      String progressTable = formatFullTableName(tenantId, PROGRESS_TABLE_NAME);
      String query = format(GET_JOBS_NOT_PARENT_SQL, jobTable, filterCriteria, jobTable, progressTable, filterCriteria,  sortBy, order);
      pgClientFactory.createInstance(tenantId).select(query, Tuple.of(limit, offset), promise);
    } catch (Exception e) {
      LOGGER.error("Error while getting Logs", e);
      promise.fail(e);
    }
    return promise.future().map(this::mapToJobExecutionDtoCollection);
  }

  @Override
  public Future<JobExecutionDtoCollection> getChildrenJobExecutionsByParentId(String parentId, int offset, int limit, String tenantId) {
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

  private Tuple mapToTuple(JobExecution jobExecution) {
    return Tuple.of(UUID.fromString(jobExecution.getId()),
      jobExecution.getHrId(),
      UUID.fromString(jobExecution.getParentJobId()),
      jobExecution.getSubordinationType().toString(),
      jobExecution.getSourcePath(),
      jobExecution.getFileName(),
      jobExecution.getProgress() == null ? null : jobExecution.getProgress().getCurrent(),
      jobExecution.getProgress() == null ? null : jobExecution.getProgress().getTotal(),
      jobExecution.getStartedDate() == null ? null : jobExecution.getStartedDate().toInstant().atOffset(ZoneOffset.UTC),
      jobExecution.getCompletedDate() == null ? null : jobExecution.getCompletedDate().toInstant().atOffset(ZoneOffset.UTC),
      jobExecution.getStatus().toString(),
      jobExecution.getUiStatus().toString(),
      jobExecution.getErrorStatus() == null ? null : jobExecution.getErrorStatus().toString(),
      jobExecution.getRunBy() == null ? null : jobExecution.getRunBy().getFirstName(),
      jobExecution.getRunBy() == null ? null : jobExecution.getRunBy().getLastName(),
      UUID.fromString(jobExecution.getUserId()),
      jobExecution.getJobProfileInfo() == null ? null : UUID.fromString(jobExecution.getJobProfileInfo().getId()),
      jobExecution.getJobProfileInfo() == null ? null : jobExecution.getJobProfileInfo().getName(),
      nonNull(jobExecution.getJobProfileInfo()) && nonNull(jobExecution.getJobProfileInfo().getDataType())
        ? jobExecution.getJobProfileInfo().getDataType().toString() : null,
      jobExecution.getJobProfileSnapshotWrapper() == null
        ? null : JsonObject.mapFrom(jobExecution.getJobProfileSnapshotWrapper()));
  }

  private JobExecutionDtoCollection mapToJobExecutionDtoCollection(RowSet<Row> rowSet) {
    JobExecutionDtoCollection jobCollection = new JobExecutionDtoCollection().withTotalRecords(0);
    for (Row row : rowSet) {
      jobCollection.getJobExecutions().add(mapRowToJobExecutionDto(row));
      jobCollection.setTotalRecords(row.getInteger(TOTAL_COUNT_FIELD));
    }
    return jobCollection;
  }

  private JobExecution mapRowToJobExecution(Row row) {
    return new JobExecution()
      .withId(row.getValue(ID_FIELD).toString())
      .withHrId(row.getInteger(HRID_FIELD))
      .withParentJobId(row.getValue(PARENT_ID_FIELD).toString())
      .withSubordinationType(row.get(JobExecution.SubordinationType.class, SUBORDINATION_TYPE_FIELD))
      .withSourcePath(row.getString(SOURCE_PATH_FIELD))
      .withFileName(row.getString(FILE_NAME_FIELD))
      .withStartedDate(Date.from(row.getOffsetDateTime(STARTED_DATE_FIELD).toInstant()))
      .withCompletedDate(mapRowToCompletedDate(row))
      .withStatus(row.get(JobExecution.Status.class, STATUS_FIELD))
      .withUiStatus(row.get(JobExecution.UiStatus.class, UI_STATUS_FIELD))
      .withErrorStatus(row.getString(ERROR_STATUS_FIELD) == null
        ? null : row.get(JobExecution.ErrorStatus.class, ERROR_STATUS_FIELD))
      .withRunBy(new RunBy()
        .withFirstName(row.getString(JOB_USER_FIRST_NAME_FIELD))
        .withLastName(row.getString(JOB_USER_LAST_NAME_FIELD)))
      .withUserId(row.getValue(USER_ID_FIELD).toString())
      .withProgress(new Progress()
        .withJobExecutionId(row.getValue(ID_FIELD).toString())
        .withCurrent(row.getInteger(PROGRESS_CURRENT_FIELD))
        .withTotal(row.getInteger(PROGRESS_TOTAL_FIELD)))
      .withJobProfileInfo(mapRowToJobProfileInfo(row))
      .withJobProfileSnapshotWrapper(row.getJsonObject(PROFILE_SNAPSHOT_WRAPPER_FIELD) == null
        ? null : row.getJsonObject(PROFILE_SNAPSHOT_WRAPPER_FIELD).mapTo(ProfileSnapshotWrapper.class));
  }

  private JobExecutionDto mapRowToJobExecutionDto(Row row) {
    return new JobExecutionDto()
      .withId(row.getValue(ID_FIELD).toString())
      .withHrId(row.getInteger(HRID_FIELD))
      .withParentJobId(row.getValue(PARENT_ID_FIELD).toString())
      .withSubordinationType(row.get(JobExecutionDto.SubordinationType.class, SUBORDINATION_TYPE_FIELD))
      .withSourcePath(row.getString(SOURCE_PATH_FIELD))
      .withFileName(row.getString(FILE_NAME_FIELD))
      .withStartedDate(Date.from(row.getOffsetDateTime(STARTED_DATE_FIELD).toInstant()))
      .withCompletedDate(mapRowToCompletedDate(row))
      .withStatus(row.get(JobExecutionDto.Status.class, STATUS_FIELD))
      .withUiStatus(row.get(JobExecutionDto.UiStatus.class, UI_STATUS_FIELD))
      .withErrorStatus(row.getString(ERROR_STATUS_FIELD) == null
        ? null : row.get(JobExecutionDto.ErrorStatus.class, ERROR_STATUS_FIELD))
      .withRunBy(new RunBy()
        .withFirstName(row.getString(JOB_USER_FIRST_NAME_FIELD))
        .withLastName(row.getString(JOB_USER_LAST_NAME_FIELD)))
      .withUserId(row.getValue(USER_ID_FIELD).toString())
      .withProgress(mapRowToProgress(row))
      .withJobProfileInfo(mapRowToJobProfileInfo(row));
  }

  private Date mapRowToCompletedDate(Row row) {
    return row.getLocalDateTime(COMPLETED_DATE_FIELD) == null
      ? null : Date.from(row.getOffsetDateTime(COMPLETED_DATE_FIELD).toInstant());
  }

  private Progress mapRowToProgress(Row row) {
    Integer processedCount = row.getInteger(CURRENTLY_PROCESSED_FIELD);
    Integer total = row.getInteger(TOTAL_FIELD);
    if (processedCount == null) {
      processedCount = row.getInteger(PROGRESS_CURRENT_FIELD);
    }
    if (total == null) {
      total = row.getInteger(PROGRESS_TOTAL_FIELD);
    }

    return new Progress()
      .withJobExecutionId(row.getValue(ID_FIELD).toString())
      .withCurrent(processedCount)
      .withTotal(total);
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
