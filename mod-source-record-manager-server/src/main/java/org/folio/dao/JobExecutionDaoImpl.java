package org.folio.dao;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.DbUtil;
import org.folio.dao.util.JobExecutionMutator;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.dao.util.SortField;
import org.folio.rest.jaxrs.model.DeleteJobExecutionsResp;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCompositeDetailDto;
import org.folio.rest.jaxrs.model.JobExecutionCompositeDetailsDto;
import org.folio.rest.jaxrs.model.JobExecutionDetail;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.JobExecutionDto.SubordinationType;
import org.folio.rest.jaxrs.model.JobExecutionDtoCollection;
import org.folio.rest.jaxrs.model.JobExecutionUserInfo;
import org.folio.rest.jaxrs.model.JobExecutionUserInfoCollection;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.JobProfileInfoCollection;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Progress;
import org.folio.rest.jaxrs.model.RunBy;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.SQLConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.ws.rs.NotFoundException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.dao.util.JobExecutionDBConstants.COMPLETED_DATE_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.CURRENTLY_PROCESSED_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.ERROR_STATUS_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.FILE_NAME_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.FIRST_NAME_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.GET_BY_ID_SQL;
import static org.folio.dao.util.JobExecutionDBConstants.GET_CHILDREN_JOBS_BY_PARENT_ID_SQL;
import static org.folio.dao.util.JobExecutionDBConstants.GET_JOBS_NOT_PARENT_SQL;
import static org.folio.dao.util.JobExecutionDBConstants.GET_RELATED_JOB_PROFILES_SQL;
import static org.folio.dao.util.JobExecutionDBConstants.GET_UNIQUE_USERS;
import static org.folio.dao.util.JobExecutionDBConstants.HRID_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.ID_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.INSERT_SQL;
import static org.folio.dao.util.JobExecutionDBConstants.JOB_PART_NUMBER;
import static org.folio.dao.util.JobExecutionDBConstants.JOB_PROFILE_COMPOSITE_DATA;
import static org.folio.dao.util.JobExecutionDBConstants.JOB_PROFILE_DATA_TYPE_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.JOB_PROFILE_HIDDEN_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.JOB_PROFILE_ID_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.JOB_PROFILE_NAME_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.JOB_USER_FIRST_NAME_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.JOB_USER_LAST_NAME_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.LAST_NAME_FIELD;
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
import static org.folio.dao.util.JobExecutionDBConstants.TOTAL_JOB_PARTS;
import static org.folio.dao.util.JobExecutionDBConstants.TOTAL_RECORDS_IN_FILE;
import static org.folio.dao.util.JobExecutionDBConstants.UI_STATUS_FIELD;
import static org.folio.dao.util.JobExecutionDBConstants.UPDATE_BY_IDS_SQL;
import static org.folio.dao.util.JobExecutionDBConstants.UPDATE_PROGRESS_SQL;
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
  private static final String ORDER_BY_PROGRESS_TOTAL = "COALESCE(p.total_records_count, progress_total)";
  private static final Set<String> CASE_INSENSITIVE_SORTABLE_FIELDS =
    Set.of("file_name", "job_profile_name", "job_user_first_name", "job_user_last_name");

  //Below constants are used for building db query related to job execution deletions
  public static final String ID = "id";
  public static final String IS_DELETED = "is_deleted";
  public static final String RETURNING_FIELD_NAMES = "returningFieldNames";
  public static final String SET_CONDITIONAL_FIELD_VALUES = "setConditionalFieldValues";
  public static final String SET_CONDITIONAL_FIELD_NAME = "setConditionalFieldName";
  public static final String SET_FIELD_VALUE = "setFieldValue";
  public static final String SET_FIELD_NAME = "setFieldName";
  public static final String TENANT_NAME = "tenantName";
  public static final String TRUE = "true";
  public static final String DB_TABLE_NAME_FIELD = "tableName";
  public static final String SELECT_IDS_FOR_DELETION = "SELECT id FROM %s.%s WHERE is_deleted = true and completed_date <= $1";
  public static final String DELETE_FROM_RELATED_TABLE = "DELETE from %s.%s where job_execution_id = ANY ($1)";
  public static final String DELETE_FROM_RELATED_TABLE_DEPRECATED_NAMING = "DELETE from %s.%s where jobexecutionid = ANY ($1)";
  public static final String DELETE_FROM_JOB_EXECUTION_TABLE = "DELETE from %s.%s where id = ANY ($1)";
  public static final String JOB_EXECUTION_SOURCE_CHUNKS_TABLE_NAME = "job_execution_source_chunks";
  public static final String JOURNAL_RECORDS_TABLE_NAME = "journal_records";
  public static final String JOB_PROFILE_COMPOSITE_DATA_STATUS = "status";
  public static final String JOB_PROFILE_COMPOSITE_DATA_TOTAL_RECORDS_COUNT = "total_records_count";
  public static final String JOB_PROFILE_COMPOSITE_DATA_CURRENTLY_PROCESSED = "currently_processed";

  @Autowired
  private PostgresClientFactory pgClientFactory;

  @Override
  public Future<JobExecutionDtoCollection> getJobExecutionsWithoutParentMultiple(JobExecutionFilter filter, List<SortField> sortFields, int offset, int limit, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String filterCriteria = filter.buildCriteria();
      String orderByClause = buildOrderByClause(sortFields);
      String jobTable = formatFullTableName(tenantId, TABLE_NAME);
      String progressTable = formatFullTableName(tenantId, PROGRESS_TABLE_NAME);
      String query = format(GET_JOBS_NOT_PARENT_SQL, jobTable, filterCriteria, jobTable, progressTable, jobTable, progressTable, filterCriteria, orderByClause);

      pgClientFactory.createInstance(tenantId).selectRead(query, Tuple.of(limit, offset), promise);
    } catch (Exception e) {
      LOGGER.warn("getJobExecutionsWithoutParentMultiple:: Error while getting Logs", e);
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
      pgClientFactory.createInstance(tenantId).selectRead(sql, queryParams, promise);
    } catch (Exception e) {
      LOGGER.warn("getChildrenJobExecutionsByParentId:: Error getting jobExecutions by parent id", e);
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
      LOGGER.warn("getJobExecutionById:: Error getting jobExecution by id", e);
      promise.fail(e);
    }
    return promise.future().map(rowSet -> rowSet.rowCount() == 0 ? Optional.empty()
      : Optional.of(mapRowToJobExecution(rowSet.iterator().next())));
  }

  @Override
  public Future<JobProfileInfoCollection> getRelatedJobProfiles(int offset, int limit, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String jobTable = formatFullTableName(tenantId, TABLE_NAME);
      String query = format(GET_RELATED_JOB_PROFILES_SQL, jobTable);
      pgClientFactory.createInstance(tenantId).selectRead(query, Tuple.of(limit, offset), promise);
    } catch (Exception e) {
      LOGGER.warn("getRelatedJobProfiles:: Error getting related Job Profiles", e);
      promise.fail(e);
    }
    return promise.future().map(this::mapRowToJobProfileInfoCollection);
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
      LOGGER.warn("updateJobExecution:: Error updating jobExecution", e);
      promise.fail(e);
    }
    return promise.future().compose(rowSet -> rowSet.rowCount() != 1
      ? Future.failedFuture(new NotFoundException(errorMessage)) : Future.succeededFuture(jobExecution));
  }

  @Override
  public Future<Void> updateJobExecutionProgress(AsyncResult<SQLConnection> connection, Progress progress, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String errorMessage = String.format("JobExecution with id '%s' was not found during progress update", progress.getJobExecutionId());
    try {
      String preparedQuery = format(UPDATE_PROGRESS_SQL, formatFullTableName(tenantId, TABLE_NAME));
      Tuple queryParams = Tuple.of(progress.getJobExecutionId(), progress.getCurrent(), progress.getTotal());
      pgClientFactory.createInstance(tenantId).execute(connection, preparedQuery, queryParams, promise);
    } catch (Exception e) {
      LOGGER.warn("updateJobExecutionProgress:: Error updating jobExecution progress, jobId: {}", progress.getJobExecutionId(), e);
      promise.fail(e);
    }
    return promise.future().compose(rowSet -> rowSet.rowCount() != 1
      ? Future.failedFuture(new NotFoundException(errorMessage)) : Future.succeededFuture());
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
        String selectForUpdate = format("SELECT * FROM %s WHERE id = $1 AND is_deleted = false LIMIT 1 FOR UPDATE", formatFullTableName(tenantId, TABLE_NAME));
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
  public Future<DeleteJobExecutionsResp> softDeleteJobExecutionsByIds(List<String> ids, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      Map<String, String> data = new HashMap<>();
      data.put(TENANT_NAME, convertToPsqlStandard(tenantId));
      data.put(DB_TABLE_NAME_FIELD, TABLE_NAME);
      data.put(SET_FIELD_NAME, IS_DELETED);
      data.put(SET_FIELD_VALUE, TRUE);
      data.put(SET_CONDITIONAL_FIELD_NAME, ID);
      data.put(SET_CONDITIONAL_FIELD_VALUES, ids.stream().collect(Collectors.joining("','")));
      data.put(RETURNING_FIELD_NAMES, ID + "," + IS_DELETED);
      String query = StrSubstitutor.replace(UPDATE_BY_IDS_SQL, data);
      pgClientFactory.createInstance(tenantId).execute(query, promise);
    } catch (Exception e) {
      LOGGER.warn("softDeleteJobExecutionsByIds:: Error deleting jobExecution by ids {}, ", ids, e);
      promise.fail(e);
    }
    return promise.future().map(this::mapRowSetToDeleteChangeManagerJobExeResp);
  }

  @Override
  public Future<JobExecutionUserInfoCollection> getRelatedUsersInfo(int offset, int limit, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String tableName = formatFullTableName(tenantId, TABLE_NAME);
      String query = format(GET_UNIQUE_USERS, tableName);
      pgClientFactory.createInstance(tenantId).selectRead(query, Tuple.of(limit, offset), promise);
    } catch (Exception e) {
      LOGGER.warn("getRelatedUsersInfo:: Error getting unique users ", e);
      promise.fail(e);
    }
    return promise.future().map(this::mapRowToJobExecutionUserInfoCollection);
  }

  private JobExecutionUserInfoCollection mapRowToJobExecutionUserInfoCollection(RowSet<Row> rowSet) {
    JobExecutionUserInfoCollection jobExecutionUserInfoCollection = new JobExecutionUserInfoCollection().withTotalRecords(0);
    for (Row row : rowSet) {
      jobExecutionUserInfoCollection.getJobExecutionUsersInfo().add(mapRowToJobExecutionUserInfoDto(row));
      jobExecutionUserInfoCollection.setTotalRecords(row.getInteger(TOTAL_COUNT_FIELD));
    }
    return jobExecutionUserInfoCollection;
  }

  private JobExecutionUserInfo mapRowToJobExecutionUserInfoDto(Row row) {
    JobExecutionUserInfo jobExecutionUserInfo = new JobExecutionUserInfo();
    jobExecutionUserInfo.setUserId(row.getUUID(USER_ID_FIELD).toString());
    jobExecutionUserInfo.setJobUserFirstName(StringUtils.defaultString(row.getString(FIRST_NAME_FIELD)));
    jobExecutionUserInfo.setJobUserLastName(StringUtils.defaultString(row.getString(LAST_NAME_FIELD)));
    return jobExecutionUserInfo;
  }

  private DeleteJobExecutionsResp mapRowSetToDeleteChangeManagerJobExeResp(RowSet<Row> rowSet) {
    DeleteJobExecutionsResp deleteJobExecutionsResp = new DeleteJobExecutionsResp();
    List<JobExecutionDetail> jobExecutionDetails = new ArrayList<>();
    rowSet.forEach(row -> {
      JobExecutionDetail executionLogDetail = new JobExecutionDetail();
      executionLogDetail.setJobExecutionId(row.getUUID(ID).toString());
      executionLogDetail.setIsDeleted(row.getBoolean(IS_DELETED));
      jobExecutionDetails.add(executionLogDetail);
    });
    deleteJobExecutionsResp.setJobExecutionDetails(jobExecutionDetails);
    return deleteJobExecutionsResp;
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
        ? null : JsonObject.mapFrom(jobExecution.getJobProfileSnapshotWrapper()),
      jobExecution.getJobProfileInfo() != null && jobExecution.getJobProfileInfo().getHidden(),
      jobExecution.getJobPartNumber(),
      jobExecution.getTotalJobParts(),
      jobExecution.getTotalRecordsInFile());
  }

  private JobExecutionDtoCollection mapToJobExecutionDtoCollection(RowSet<Row> rowSet) {
    JobExecutionDtoCollection jobCollection = new JobExecutionDtoCollection().withTotalRecords(0);
    rowSet.iterator().forEachRemaining(row -> {
      jobCollection.getJobExecutions().add(mapRowToJobExecutionDto(row));
      jobCollection.setTotalRecords(row.getInteger(TOTAL_COUNT_FIELD));
    });
    return jobCollection;
  }

  private JobProfileInfoCollection mapRowToJobProfileInfoCollection(RowSet<Row> rowSet) {
    JobProfileInfoCollection jobCollection = new JobProfileInfoCollection().withTotalRecords(0);
    rowSet.iterator().forEachRemaining(row -> {
      jobCollection.getJobProfilesInfo().add(mapRowToJobProfileInfo(row));
      jobCollection.setTotalRecords(row.getInteger(TOTAL_COUNT_FIELD));
    });
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
        ? null : row.getJsonObject(PROFILE_SNAPSHOT_WRAPPER_FIELD).mapTo(ProfileSnapshotWrapper.class))
      .withJobPartNumber(row.getInteger(JOB_PART_NUMBER))
      .withTotalJobParts(row.getInteger(TOTAL_JOB_PARTS))
      .withTotalRecordsInFile(row.getInteger(TOTAL_RECORDS_IN_FILE));
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
      .withJobProfileInfo(mapRowToJobProfileInfo(row))
      .withJobPartNumber(row.getInteger(JOB_PART_NUMBER))
      .withTotalJobParts(row.getInteger(TOTAL_JOB_PARTS))
      .withTotalRecordsInFile(row.getInteger(TOTAL_RECORDS_IN_FILE))
      .withCompositeDetails(mapToJobExecutionCompositeDetailsDto(row));
  }

  private Date mapRowToCompletedDate(Row row) {
    return row.getLocalDateTime(COMPLETED_DATE_FIELD) == null
      ? null : Date.from(row.getOffsetDateTime(COMPLETED_DATE_FIELD).toInstant());
  }

  private Progress mapRowToProgress(Row row) {
    if (row.get(JobExecutionDto.SubordinationType.class, SUBORDINATION_TYPE_FIELD).equals(SubordinationType.COMPOSITE_PARENT)) {
      return mapRowToProgressComposite(row);
    }

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

  private Progress mapRowToProgressComposite(Row row) {
    JsonArray compositeData = row.getJsonArray(JOB_PROFILE_COMPOSITE_DATA);

    if (Objects.nonNull(compositeData) && !compositeData.isEmpty()) {
      Progress progressDto = new Progress()
        .withJobExecutionId(row.getValue(ID_FIELD).toString());

      int processed = 0;
      int total = 0;

      for (Object o : compositeData) {
        JsonObject jo = (JsonObject) o;

        processed += Optional.ofNullable(jo.getInteger(JOB_PROFILE_COMPOSITE_DATA_CURRENTLY_PROCESSED)).orElse(0);
        total += Optional.ofNullable(jo.getInteger(JOB_PROFILE_COMPOSITE_DATA_TOTAL_RECORDS_COUNT)).orElse(0);
      }

      return progressDto.withCurrent(processed).withTotal(total);
    }

    return null;
  }

  private JobProfileInfo mapRowToJobProfileInfo(Row row) {
    UUID profileId = row.getUUID(JOB_PROFILE_ID_FIELD);
    if (Objects.nonNull(profileId)) {
      return new JobProfileInfo()
        .withId(profileId.toString())
        .withName(row.getString(JOB_PROFILE_NAME_FIELD))
        .withHidden(row.getBoolean(JOB_PROFILE_HIDDEN_FIELD))
        .withDataType(Objects.isNull(row.getString(JOB_PROFILE_DATA_TYPE_FIELD))
          ? null : JobProfileInfo.DataType.fromValue(row.getString(JOB_PROFILE_DATA_TYPE_FIELD)));
    }
    return null;
  }

  private JobExecutionCompositeDetailsDto mapToJobExecutionCompositeDetailsDto(Row row) {
    if (row.getColumnIndex(JOB_PROFILE_COMPOSITE_DATA) == -1) {
      return null;
    }
    JsonArray compositeData = row.getJsonArray(JOB_PROFILE_COMPOSITE_DATA);
    if (Objects.nonNull(compositeData) && !compositeData.isEmpty()) {
      var detailsDto = new JobExecutionCompositeDetailsDto();

      compositeData.forEach((Object o) -> {
        JsonObject jo = (JsonObject) o;
        JobExecutionDto.Status status = JobExecutionDto.Status.valueOf(jo.getString(JOB_PROFILE_COMPOSITE_DATA_STATUS));

        JobExecutionCompositeDetailDto stateDto = new JobExecutionCompositeDetailDto()
          .withChunksCount(jo.getInteger("cnt"))
          .withTotalRecordsCount(jo.getInteger(JOB_PROFILE_COMPOSITE_DATA_TOTAL_RECORDS_COUNT))
          .withCurrentlyProcessedCount(jo.getInteger(JOB_PROFILE_COMPOSITE_DATA_CURRENTLY_PROCESSED));

        switch (status) {
          case NEW:
            detailsDto.setNewState(stateDto);
            break;
          case FILE_UPLOADED:
            detailsDto.setFileUploadedState(stateDto);
            break;
          case PARSING_IN_PROGRESS:
            detailsDto.setParsingInProgressState(stateDto);
            break;
          case PARSING_FINISHED:
            detailsDto.setParsingFinishedState(stateDto);
            break;
          case PROCESSING_IN_PROGRESS:
            detailsDto.setProcessingInProgressState(stateDto);
            break;
          case PROCESSING_FINISHED:
            detailsDto.setProcessingFinishedState(stateDto);
            break;
          case COMMIT_IN_PROGRESS:
            detailsDto.setCommitInProgressState(stateDto);
            break;
          case COMMITTED:
            detailsDto.setCommittedState(stateDto);
            break;
          case ERROR:
            detailsDto.setErrorState(stateDto);
            break;
          case DISCARDED:
            detailsDto.setDiscardedState(stateDto);
            break;
          case CANCELLED:
            detailsDto.setCancelledState(stateDto);
            break;
          default:
            throw new IllegalStateException("Invalid child status: " + status);
        }
      });
      return detailsDto;
    }
    return null;
  }

  private String formatFullTableName(String tenantId, String table) {
    return format("%s.%s", convertToPsqlStandard(tenantId), table);
  }

  private String buildOrderByClause(List<SortField> sortFields) {
    if (CollectionUtils.isEmpty(sortFields)) {
      return EMPTY;
    }
    return sortFields.stream()
      .map(sortField -> CASE_INSENSITIVE_SORTABLE_FIELDS.contains(sortField.getField())
        ? wrapWithLowerCase(sortField)
        : sortField.toString())
      .map(sortField -> sortField.contains(PROGRESS_TOTAL_FIELD)
        ? sortField.replace(PROGRESS_TOTAL_FIELD, ORDER_BY_PROGRESS_TOTAL)
        : sortField)
      .collect(Collectors.joining(", ", "ORDER BY ", EMPTY));
  }

  private String wrapWithLowerCase(SortField sortField) {
    return String.format("lower(%s) %s", sortField.getField(), sortField.getOrder());
  }

  @Override
  public Future<Boolean> hardDeleteJobExecutions(long diffNumberOfDays, String tenantId) {
    PostgresClient postgresClient = pgClientFactory.createInstance(tenantId);
    return DbUtil.executeInTransaction(postgresClient, sqlConnection ->
      fetchJobExecutionIdsConsideredForDeleting(tenantId, diffNumberOfDays, sqlConnection, postgresClient)
        .compose(rowSet -> {
          if (rowSet.rowCount() < 1) {
            LOGGER.info("hardDeleteJobExecutions:: Jobs marked as deleted and older than {} days not found", diffNumberOfDays);
            return Future.succeededFuture();
          }
          return mapRowsetValuesToListOfString(rowSet);
        })
        .compose(jobExecutionIds -> {
          if (CollectionUtils.isEmpty(jobExecutionIds)) {
            return Future.succeededFuture();
          }

          UUID[] uuids = jobExecutionIds.stream().map(UUID::fromString).collect(Collectors.toList()).toArray(UUID[]::new);

          Future<RowSet<Row>> jobExecutionProgressFuture = Future.future(rowSetPromise -> deleteFromRelatedTable(PROGRESS_TABLE_NAME, uuids, sqlConnection, tenantId, rowSetPromise, postgresClient));
          Future<RowSet<Row>> jobExecutionSourceChunksFuture = Future.future(rowSetPromise -> deleteFromRelatedTableWithDeprecatedNaming(JOB_EXECUTION_SOURCE_CHUNKS_TABLE_NAME, uuids, sqlConnection, tenantId, rowSetPromise, postgresClient));
          Future<RowSet<Row>> journalRecordsFuture = Future.future(rowSetPromise -> deleteFromRelatedTable(JOURNAL_RECORDS_TABLE_NAME, uuids, sqlConnection, tenantId, rowSetPromise, postgresClient));
          return CompositeFuture.all(jobExecutionProgressFuture, jobExecutionSourceChunksFuture, journalRecordsFuture)
            .compose(ar -> Future.<RowSet<Row>>future(rowSetPromise -> deleteFromJobExecutionTable(uuids, sqlConnection, tenantId, rowSetPromise, postgresClient)))
            .map(true);
        }));
  }

  private Future<List<String>> mapRowsetValuesToListOfString(RowSet<Row> rowset) {
    List<String> uuidsToBeDeleted = new ArrayList<>();
    rowset.iterator().forEachRemaining(o -> uuidsToBeDeleted.add(String.valueOf(o.getValue(ID))));
    return Future.succeededFuture(uuidsToBeDeleted);
  }

  private Future<RowSet<Row>> fetchJobExecutionIdsConsideredForDeleting(String tenantId, long diffNumberOfDays, AsyncResult<SQLConnection> connection, PostgresClient postgresClient) {
    String selectForDeletion = format(SELECT_IDS_FOR_DELETION, convertToPsqlStandard(tenantId), TABLE_NAME);
    Tuple queryParams = Tuple.of(LocalDateTime.now().minus(diffNumberOfDays, ChronoUnit.DAYS).atOffset(ZoneOffset.UTC));
    Promise<RowSet<Row>> selectResult = Promise.promise();
    postgresClient.execute(connection, selectForDeletion, queryParams, selectResult);
    return selectResult.future();
  }

  private void deleteFromRelatedTable(String tableName, UUID[] uuids, AsyncResult<SQLConnection> connection, String tenantId, Promise<RowSet<Row>> promise, PostgresClient postgresClient) {
    String deleteQuery = format(DELETE_FROM_RELATED_TABLE, convertToPsqlStandard(tenantId), tableName);
    Tuple queryParams = Tuple.of(uuids);
    postgresClient.execute(connection, deleteQuery, queryParams, promise);
  }

  private void deleteFromRelatedTableWithDeprecatedNaming(String tableName, UUID[] uuids, AsyncResult<SQLConnection> connection, String tenantId, Promise<RowSet<Row>> promise, PostgresClient postgresClient) {
    String deleteQuery = format(DELETE_FROM_RELATED_TABLE_DEPRECATED_NAMING, convertToPsqlStandard(tenantId), tableName);
    Tuple queryParams = Tuple.of(uuids);
    postgresClient.execute(connection, deleteQuery, queryParams, promise);
  }

  private void deleteFromJobExecutionTable(UUID[] uuids, AsyncResult<SQLConnection> connection, String tenantId, Promise<RowSet<Row>> promise, PostgresClient postgresClient) {
    String deleteQuery = format(DELETE_FROM_JOB_EXECUTION_TABLE, convertToPsqlStandard(tenantId), TABLE_NAME);
    Tuple queryParams = Tuple.of(uuids);
    postgresClient.execute(connection, deleteQuery, queryParams, promise);
  }
}
