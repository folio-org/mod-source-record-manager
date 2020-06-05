package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.ws.rs.BadRequestException;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.ActionLog;
import org.folio.rest.jaxrs.model.JobExecutionLogDto;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.JournalRecord.ActionStatus;
import org.folio.rest.jaxrs.model.JournalRecord.ActionType;
import org.folio.rest.jaxrs.model.JournalRecord.EntityType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.rest.persist.PostgresClient.convertToPsqlStandard;

@Repository
public class JournalRecordDaoImpl implements JournalRecordDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(JournalRecordDaoImpl.class);
  private final Set<String> sortableFields = Collections.unmodifiableSet(new HashSet<>(Arrays.asList("source_record_order", "action_type", "error")));

  private static final String JOURNAL_RECORDS_TABLE = "journal_records";
  private static final String INSERT_SQL = "INSERT INTO %s.%s (id, job_execution_id, source_id, source_record_order, entity_type, entity_id, entity_hrid, action_type, action_status, error, action_date) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
  private static final String SELECT_BY_JOB_EXECUTION_ID_QUERY = "SELECT * FROM %s.%s WHERE job_execution_id = $1";
  private static final String ORDER_BY_PATTERN = " ORDER BY %s %s";
  private static final String DELETE_BY_JOB_EXECUTION_ID_QUERY = "DELETE FROM %s.%s WHERE job_execution_id = $1";
  private static final String GET_JOB_LOG_BY_JOB_EXECUTION_ID_QUERY = "SELECT job_execution_id, entity_type, action_type, " +
    "COUNT(*) FILTER (WHERE action_status = 'COMPLETED') AS total_completed, " +
    "COUNT(*) FILTER (WHERE action_status = 'ERROR') AS total_failed " +
    "FROM %s.%s WHERE job_execution_id = $1 AND action_type != 'ERROR' " +
    "GROUP BY (job_execution_id, entity_type, action_type)";

  @Autowired
  private PostgresClientFactory pgClientFactory;

  @Override
  public Future<String> save(JournalRecord journalRecord, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      journalRecord.withId(UUID.randomUUID().toString());
      String query = format(INSERT_SQL, convertToPsqlStandard(tenantId), JOURNAL_RECORDS_TABLE);
      pgClientFactory.createInstance(tenantId).execute(query, prepareInsertQueryParameters(journalRecord), promise);
    } catch (Exception e) {
      LOGGER.error("Error saving JournalRecord entity", e);
      promise.fail(e);
    }
    return promise.future().map(journalRecord.getId());
  }

  private Tuple prepareInsertQueryParameters(JournalRecord journalRecord) {
    return Tuple.of(UUID.fromString(journalRecord.getId()),
      UUID.fromString(journalRecord.getJobExecutionId()),
      UUID.fromString(journalRecord.getSourceId()),
      journalRecord.getSourceRecordOrder(),
      journalRecord.getEntityType().toString(),
      journalRecord.getEntityId(),
      journalRecord.getEntityHrId() != null ? journalRecord.getEntityHrId() : EMPTY,
      journalRecord.getActionType().toString(),
      journalRecord.getActionStatus().toString(),
      journalRecord.getError() != null ? journalRecord.getError() : EMPTY,
      Timestamp.from(journalRecord.getActionDate().toInstant()).toLocalDateTime());
  }

  @Override
  public Future<List<JournalRecord>> getByJobExecutionId(String jobExecutionId, String sortBy, String order, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      StringBuilder queryBuilder = new StringBuilder(format(SELECT_BY_JOB_EXECUTION_ID_QUERY, convertToPsqlStandard(tenantId), JOURNAL_RECORDS_TABLE));
      if (sortBy != null) {
        queryBuilder.append(prepareSortingClause(sortBy, order));
      }
      Tuple queryParams = Tuple.of(UUID.fromString(jobExecutionId));
      pgClientFactory.createInstance(tenantId).select(queryBuilder.toString(), queryParams, promise);
    } catch (Exception e) {
      LOGGER.error("Error getting JournalRecords by jobExecution id", e);
      promise.fail(e);
    }
    return promise.future().map(this::mapResultSetToJournalRecordsList);
  }

  @Override
  public Future<Boolean> deleteByJobExecutionId(String jobExecutionId, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String query = format(DELETE_BY_JOB_EXECUTION_ID_QUERY, convertToPsqlStandard(tenantId), JOURNAL_RECORDS_TABLE);
    Tuple queryParams = Tuple.of(UUID.fromString(jobExecutionId));
    pgClientFactory.createInstance(tenantId).execute(query, queryParams, promise);
    return promise.future().map(updateResult -> updateResult.rowCount() >= 1);
  }

  @Override
  public Future<JobExecutionLogDto> getJobExecutionLogDto(String jobExecutionId, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String query = format(GET_JOB_LOG_BY_JOB_EXECUTION_ID_QUERY, convertToPsqlStandard(tenantId), JOURNAL_RECORDS_TABLE);
    Tuple queryParams = Tuple.of(UUID.fromString(jobExecutionId));
    pgClientFactory.createInstance(tenantId).select(query, queryParams, promise);
    return promise.future().map(this::mapResultSetToJobExecutionLogDto);
  }

  private List<JournalRecord> mapResultSetToJournalRecordsList(RowSet<Row> resultSet) {
    List<JournalRecord> journalRecords = new ArrayList<>();
    resultSet.forEach(row -> journalRecords.add(mapRowJsonToJournalRecord(row)));
    return journalRecords;
  }

  private JournalRecord mapRowJsonToJournalRecord(Row row) {
    return new JournalRecord()
      .withId(row.getValue("id").toString())
      .withJobExecutionId(row.getValue("job_execution_id").toString())
      .withSourceId(row.getValue("source_id").toString())
      .withSourceRecordOrder(row.getInteger("source_record_order"))
      .withEntityType(EntityType.valueOf(row.getString("entity_type")))
      .withEntityId(row.getString("entity_id"))
      .withEntityHrId(row.getString("entity_hrid"))
      .withActionType(ActionType.valueOf(row.getString("action_type")))
      .withActionStatus(ActionStatus.valueOf(row.getString("action_status")))
      .withError(row.getString("error"))
      .withActionDate(Date.from(LocalDateTime.parse(row.getValue("action_date").toString()).toInstant(ZoneOffset.UTC)));
  }

  private JobExecutionLogDto mapResultSetToJobExecutionLogDto(RowSet<Row> resultSet) {
    JobExecutionLogDto jobExecutionSummary = new JobExecutionLogDto();
    resultSet.forEach(row -> {
      ActionLog actionLog = new ActionLog()
        .withEntityType(row.getString("entity_type"))
        .withActionType(row.getString("action_type"))
        .withTotalCompleted(row.getInteger("total_completed"))
        .withTotalFailed(row.getInteger("total_failed"));

      jobExecutionSummary.withJobExecutionId(row.getValue("job_execution_id").toString());
      jobExecutionSummary.getJobExecutionResultLogs().add(actionLog);
    });
    return jobExecutionSummary;
  }

  private String prepareSortingClause(String sortBy, String order) {
    if (!sortableFields.contains(sortBy)) {
      throw new BadRequestException(format("The specified field for sorting journal records is invalid: '%s'", sortBy));
    }
    return format(ORDER_BY_PATTERN, sortBy, order);
  }

}
