package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.Tuple;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.EntityProcessingSummary;
import org.folio.rest.jaxrs.model.JobExecutionSummaryDto;
import org.folio.rest.jaxrs.model.JobLogEntryDto;
import org.folio.rest.jaxrs.model.JobLogEntryDtoCollection;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.JournalRecord.ActionStatus;
import org.folio.rest.jaxrs.model.JournalRecord.ActionType;
import org.folio.rest.jaxrs.model.JournalRecord.EntityType;
import org.folio.rest.jaxrs.model.ProcessedEntityInfo;
import org.folio.rest.jaxrs.model.ProcessedHoldingsInfo;
import org.folio.rest.jaxrs.model.ProcessedItemInfo;
import org.folio.rest.jaxrs.model.RecordProcessingLogDto;
import org.folio.rest.jaxrs.model.RelatedInvoiceLineInfo;
import org.folio.rest.jaxrs.model.RelatedPoLineInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.dao.util.JournalRecordsColumns.ACTION_DATE;
import static org.folio.dao.util.JournalRecordsColumns.ACTION_STATUS;
import static org.folio.dao.util.JournalRecordsColumns.ACTION_TYPE;
import static org.folio.dao.util.JournalRecordsColumns.AUTHORITY_ACTION_STATUS;
import static org.folio.dao.util.JournalRecordsColumns.AUTHORITY_ENTITY_ERROR;
import static org.folio.dao.util.JournalRecordsColumns.AUTHORITY_ENTITY_ID;
import static org.folio.dao.util.JournalRecordsColumns.ENTITY_HRID;
import static org.folio.dao.util.JournalRecordsColumns.ENTITY_ID;
import static org.folio.dao.util.JournalRecordsColumns.ENTITY_TYPE;
import static org.folio.dao.util.JournalRecordsColumns.ERROR;
import static org.folio.dao.util.JournalRecordsColumns.HOLDINGS_ACTION_STATUS;
import static org.folio.dao.util.JournalRecordsColumns.HOLDINGS_ENTITY_ERROR;
import static org.folio.dao.util.JournalRecordsColumns.HOLDINGS_ENTITY_HRID;
import static org.folio.dao.util.JournalRecordsColumns.HOLDINGS_ENTITY_ID;
import static org.folio.dao.util.JournalRecordsColumns.HOLDINGS_ID;
import static org.folio.dao.util.JournalRecordsColumns.HOLDINGS_PERMANENT_LOCATION_ID;
import static org.folio.dao.util.JournalRecordsColumns.ID;
import static org.folio.dao.util.JournalRecordsColumns.INSTANCE_ACTION_STATUS;
import static org.folio.dao.util.JournalRecordsColumns.INSTANCE_ENTITY_ERROR;
import static org.folio.dao.util.JournalRecordsColumns.INSTANCE_ENTITY_HRID;
import static org.folio.dao.util.JournalRecordsColumns.INSTANCE_ENTITY_ID;
import static org.folio.dao.util.JournalRecordsColumns.INSTANCE_ID;
import static org.folio.dao.util.JournalRecordsColumns.INVOICE_ACTION_STATUS;
import static org.folio.dao.util.JournalRecordsColumns.INVOICE_ENTITY_ERROR;
import static org.folio.dao.util.JournalRecordsColumns.INVOICE_ENTITY_HRID;
import static org.folio.dao.util.JournalRecordsColumns.INVOICE_ENTITY_ID;
import static org.folio.dao.util.JournalRecordsColumns.INVOICE_LINE_ACTION_STATUS;
import static org.folio.dao.util.JournalRecordsColumns.INVOICE_LINE_ENTITY_ERROR;
import static org.folio.dao.util.JournalRecordsColumns.INVOICE_LINE_ENTITY_HRID;
import static org.folio.dao.util.JournalRecordsColumns.INVOICE_LINE_ENTITY_ID;
import static org.folio.dao.util.JournalRecordsColumns.INVOICE_LINE_JOURNAL_RECORD_ID;
import static org.folio.dao.util.JournalRecordsColumns.INVOICE_LINE_NUMBER;
import static org.folio.dao.util.JournalRecordsColumns.ITEM_ACTION_STATUS;
import static org.folio.dao.util.JournalRecordsColumns.ITEM_ENTITY_ERROR;
import static org.folio.dao.util.JournalRecordsColumns.ITEM_ENTITY_HRID;
import static org.folio.dao.util.JournalRecordsColumns.ITEM_ENTITY_ID;
import static org.folio.dao.util.JournalRecordsColumns.JOB_EXECUTION_ID;
import static org.folio.dao.util.JournalRecordsColumns.SOURCE_ENTITY_ERROR;
import static org.folio.dao.util.JournalRecordsColumns.SOURCE_ID;
import static org.folio.dao.util.JournalRecordsColumns.SOURCE_RECORD_ACTION_STATUS;
import static org.folio.dao.util.JournalRecordsColumns.SOURCE_RECORD_ORDER;
import static org.folio.dao.util.JournalRecordsColumns.TITLE;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_AUTHORITIES_ERRORS;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_COUNT;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_CREATED_AUTHORITIES;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_CREATED_HOLDINGS;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_CREATED_INSTANCES;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_CREATED_INVOICES;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_CREATED_ITEMS;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_CREATED_ORDERS;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_CREATED_SOURCE_RECORDS;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_DISCARDED_AUTHORITIES;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_DISCARDED_HOLDINGS;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_DISCARDED_INSTANCES;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_DISCARDED_INVOICES;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_DISCARDED_ITEMS;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_DISCARDED_ORDERS;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_DISCARDED_SOURCE_RECORDS;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_ERRORS;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_HOLDINGS_ERRORS;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_INSTANCES_ERRORS;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_INVOICES_ERRORS;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_ITEMS_ERRORS;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_ORDERS_ERRORS;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_SOURCE_RECORDS_ERRORS;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_UPDATED_AUTHORITIES;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_UPDATED_HOLDINGS;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_UPDATED_INSTANCES;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_UPDATED_INVOICES;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_UPDATED_ITEMS;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_UPDATED_ORDERS;
import static org.folio.dao.util.JournalRecordsColumns.TOTAL_UPDATED_SOURCE_RECORDS;
import static org.folio.dao.util.JournalRecordsColumns.ORDER_ID;
import static org.folio.dao.util.JournalRecordsColumns.PO_LINE_ACTION_STATUS;
import static org.folio.dao.util.JournalRecordsColumns.PO_LINE_ENTITY_ID;
import static org.folio.dao.util.JournalRecordsColumns.PO_LINE_ENTITY_HRID;
import static org.folio.dao.util.JournalRecordsColumns.PO_LINE_ENTITY_ERROR;
import static org.folio.rest.jaxrs.model.JobLogEntryDto.SourceRecordType.MARC_HOLDINGS;
import static org.folio.rest.persist.PostgresClient.convertToPsqlStandard;

@Repository
public class JournalRecordDaoImpl implements JournalRecordDao {

  private static final Logger LOGGER = LogManager.getLogger();
  public static final String SOURCE_RECORD_ENTITY_TYPE = "source_record_entity_type";
  public static final String ORDER_ENTITY_ID = "order_entity_id";
  private final Set<String> sortableFields = Set.of("source_record_order", "action_type", "error");
  private final Set<String> jobLogEntrySortableFields = Set.of("source_record_order", "title", "source_record_action_status",
    "instance_action_status", "holdings_action_status", "item_action_status", "order_action_status", "invoice_action_status", "error");

  private static final String JOURNAL_RECORDS_TABLE = "journal_records";
  private static final String INSERT_SQL = "INSERT INTO %s.%s (id, job_execution_id, source_id, source_record_order, entity_type, entity_id, entity_hrid, action_type, action_status, error, action_date, title, instance_id, holdings_id, order_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)";
  private static final String SELECT_BY_JOB_EXECUTION_ID_QUERY = "SELECT * FROM %s.%s WHERE job_execution_id = $1";
  private static final String ORDER_BY_PATTERN = " ORDER BY %s %s";
  private static final String DELETE_BY_JOB_EXECUTION_ID_QUERY = "DELETE FROM %s.%s WHERE job_execution_id = $1";
  private static final String GET_JOB_LOG_ENTRIES_BY_JOB_EXECUTION_ID_QUERY = "SELECT * FROM get_job_log_entries('%s', '%s', '%s', %s, %s, %b, '%s')";
  private static final String GET_JOB_LOG_RECORD_PROCESSING_ENTRIES_BY_JOB_EXECUTION_AND_RECORD_ID_QUERY = "SELECT * FROM get_record_processing_log('%s', '%s')";
  private static final String GET_JOB_SUMMARY_QUERY = "SELECT * FROM get_job_execution_summary('%s')";
  private static final String UPDATE_ERROR_JOURNAL_RECORD_BY_ORDER_ID_AND_JOB_EXECUTION_ID = "UPDATE %s.%s SET error = $1  WHERE order_id = $2 AND job_execution_id = $3;";

  @Autowired
  private PostgresClientFactory pgClientFactory;

  @Override
  public Future<String> save(JournalRecord journalRecord, String tenantId) {
    LOGGER.info("save:: Trying to save JournalRecord entity to the {} table", JOURNAL_RECORDS_TABLE);
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      journalRecord.withId(UUID.randomUUID().toString());
      String query = format(INSERT_SQL, convertToPsqlStandard(tenantId), JOURNAL_RECORDS_TABLE);
      LOGGER.trace("JournalRecordDaoImpl:: save query = {};", query);
      pgClientFactory.createInstance(tenantId).execute(query, prepareInsertQueryParameters(journalRecord), promise);
    } catch (Exception e) {
      LOGGER.warn("save:: Error saving JournalRecord entity", e);
      promise.fail(e);
    }
    return promise.future().map(journalRecord.getId())
      .onFailure(e -> LOGGER.warn("save:: Error saving JournalRecord entity", e));
  }

  @Override
  public Future<List<RowSet<Row>>> saveBatch(List<JournalRecord> journalRecords, String tenantId) {
    LOGGER.info("saveBatch:: Trying to save list of JournalRecord entities to the {} table", JOURNAL_RECORDS_TABLE);
    Promise<List<RowSet<Row>>> promise = Promise.promise();
    try {
      List<Tuple> tupleList = journalRecords.stream().map(this::prepareInsertQueryParameters).collect(Collectors.toList());
      String query = format(INSERT_SQL, convertToPsqlStandard(tenantId), JOURNAL_RECORDS_TABLE);
      LOGGER.trace("saveBatch:: JournalRecordDaoImpl::saveBatch query = {}; tuples = {}", query, tupleList);
      pgClientFactory.createInstance(tenantId).execute(query, tupleList, promise);
    } catch (Exception e) {
      LOGGER.warn("saveBatch:: Error saving JournalRecord entities", e);
      promise.fail(e);
    }
    return promise.future().onFailure(e -> LOGGER.warn("saveBatch:: Error saving JournalRecord entities", e));
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
      Timestamp.from(journalRecord.getActionDate().toInstant()).toLocalDateTime(),
      journalRecord.getTitle(),
      journalRecord.getInstanceId(),
      journalRecord.getHoldingsId(),
      journalRecord.getOrderId());
  }

  @Override
  public Future<List<JournalRecord>> getByJobExecutionId(String jobExecutionId, String sortBy, String order, String tenantId) {
    LOGGER.trace("getByJobExecutionId:: Trying to get list of JournalRecord entities by jobExecutionId = {} from the {} table", jobExecutionId, JOURNAL_RECORDS_TABLE);
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      StringBuilder queryBuilder = new StringBuilder(format(SELECT_BY_JOB_EXECUTION_ID_QUERY, convertToPsqlStandard(tenantId), JOURNAL_RECORDS_TABLE));
      if (sortBy != null) {
        queryBuilder.append(prepareSortingClause(sortBy, order));
      }
      String query = queryBuilder.toString();
      Tuple queryParams = Tuple.of(UUID.fromString(jobExecutionId));
      LOGGER.trace("getByJobExecutionId:: JournalRecordDaoImpl::getByJobExecutionId query = {}; tuple = {}", query, queryParams);
      pgClientFactory.createInstance(tenantId).selectRead(query, queryParams, promise);
    } catch (Exception e) {
      LOGGER.warn("getByJobExecutionId:: Error getting JournalRecord entities by jobExecutionId = {}", jobExecutionId, e);
      promise.fail(e);
    }
    return promise.future().map(this::mapResultSetToJournalRecordsList);
  }

  @Override
  public Future<Boolean> deleteByJobExecutionId(String jobExecutionId, String tenantId) {
    LOGGER.debug("deleteByJobExecutionId:: Trying to delete row from the {} table by jobExecutionId = {}", JOURNAL_RECORDS_TABLE, jobExecutionId);
    Promise<RowSet<Row>> promise = Promise.promise();
    String query = format(DELETE_BY_JOB_EXECUTION_ID_QUERY, convertToPsqlStandard(tenantId), JOURNAL_RECORDS_TABLE);
    Tuple queryParams = Tuple.of(UUID.fromString(jobExecutionId));
    LOGGER.trace("JournalRecordDaoImpl::deleteByJobExecutionId query = {}; tuple = {}", query, queryParams);
    pgClientFactory.createInstance(tenantId).execute(query, queryParams, promise);
    return promise.future().map(updateResult -> updateResult.rowCount() >= 1);
  }

  @Override
  public Future<JobLogEntryDtoCollection> getJobLogEntryDtoCollection(String jobExecutionId, String sortBy, String order, boolean errorsOnly, String entityType, int limit, int offset, String tenantId) {
    LOGGER.trace("getJobLogEntryDtoCollection:: Trying to get JobLogEntryDtoCollection entity by jobExecutionId = {}", jobExecutionId);
    if (!jobLogEntrySortableFields.contains(sortBy)) {
      return Future.failedFuture(new BadRequestException(format("The specified field for sorting job log entries is invalid: '%s'", sortBy)));
    }
    Promise<RowSet<Row>> promise = Promise.promise();
    String query = format(GET_JOB_LOG_ENTRIES_BY_JOB_EXECUTION_ID_QUERY, jobExecutionId, sortBy, order, limit, offset, errorsOnly, entityType);
    LOGGER.trace("JournalRecordDaoImpl::getJobLogEntryDtoCollection query = {};", query);
    pgClientFactory.createInstance(tenantId).select(query, promise);
    return promise.future().map(this::mapRowSetToJobLogDtoCollection);
  }

  @Override
  public Future<RecordProcessingLogDto> getRecordProcessingLogDto(String jobExecutionId, String recordId, String tenantId) {
    LOGGER.trace("getRecordProcessingLogDto:: Trying to get RecordProcessingLogDto entity by jobExecutionId = {} and recordId = {}", jobExecutionId, recordId);
    Promise<RowSet<Row>> promise = Promise.promise();
    String query = format(GET_JOB_LOG_RECORD_PROCESSING_ENTRIES_BY_JOB_EXECUTION_AND_RECORD_ID_QUERY, jobExecutionId, recordId);
    LOGGER.trace("JournalRecordDaoImpl::getRecordProcessingLogDto query = {};", query);
    pgClientFactory.createInstance(tenantId).select(query, promise);
    return promise.future().map(this::mapRowSetToRecordProcessingLogDto);
  }

  @Override
  public Future<Optional<JobExecutionSummaryDto>> getJobExecutionSummaryDto(String jobExecutionId, String tenantId) {
    LOGGER.trace("getJobExecutionSummaryDto:: Trying to get JobExecutionSummaryDto by jobExecutionId: '{}' from the {} table", jobExecutionId, JOURNAL_RECORDS_TABLE);
    Promise<RowSet<Row>> promise = Promise.promise();
    String query = format(GET_JOB_SUMMARY_QUERY, jobExecutionId);
    LOGGER.trace("JournalRecordDaoImpl::getJobExecutionSummaryDto query: {}", query);
    pgClientFactory.createInstance(tenantId).select(query, promise);
    return promise.future().map(rows -> rows.rowCount() > 0
      ? Optional.of(mapRowToJobExecutionSummaryDto(rows.iterator().next())) : Optional.empty());
  }

  @Override
  public Future<Integer> updateErrorJournalRecordsByOrderIdAndJobExecution(String jobExecutionId, String orderId, String error, String tenantId) {
    LOGGER.info("updateErrorJournalRecordsByOrderIdAndJobExecution:: Trying to update JournalRecord entities by jobExecutionId: '{}' in the {} table", jobExecutionId, JOURNAL_RECORDS_TABLE);
    Promise<RowSet<Row>> promise = Promise.promise();
    String query = format(UPDATE_ERROR_JOURNAL_RECORD_BY_ORDER_ID_AND_JOB_EXECUTION_ID, convertToPsqlStandard(tenantId), JOURNAL_RECORDS_TABLE);
    LOGGER.trace("JournalRecordDaoImpl::updateErrorJournalRecordsByOrderIdAndJobExecution query = {};", query);
    Tuple queryParams = Tuple.of(error, orderId, UUID.fromString(jobExecutionId));
    pgClientFactory.createInstance(tenantId).execute(query, queryParams, promise);
    return promise.future().map(SqlResult::rowCount);
  }

  private List<JournalRecord> mapResultSetToJournalRecordsList(RowSet<Row> resultSet) {
    List<JournalRecord> journalRecords = new ArrayList<>();
    resultSet.forEach(row -> journalRecords.add(mapRowJsonToJournalRecord(row)));
    return journalRecords;
  }

  private JournalRecord mapRowJsonToJournalRecord(Row row) {
    return new JournalRecord()
      .withId(row.getValue(ID).toString())
      .withJobExecutionId(row.getValue(JOB_EXECUTION_ID).toString())
      .withSourceId(row.getValue(SOURCE_ID).toString())
      .withSourceRecordOrder(row.getInteger(SOURCE_RECORD_ORDER))
      .withTitle(row.getString(TITLE))
      .withEntityType(EntityType.valueOf(row.getString(ENTITY_TYPE)))
      .withEntityId(row.getString(ENTITY_ID))
      .withInstanceId(row.getString(INSTANCE_ID))
      .withHoldingsId(row.getString(HOLDINGS_ID))
      .withOrderId(row.getString(ORDER_ID))
      .withEntityHrId(row.getString(ENTITY_HRID))
      .withActionType(ActionType.valueOf(row.getString(ACTION_TYPE)))
      .withActionStatus(ActionStatus.valueOf(row.getString(ACTION_STATUS)))
      .withError(row.getString(ERROR))
      .withActionDate(Date.from(LocalDateTime.parse(row.getValue(ACTION_DATE).toString()).toInstant(ZoneOffset.UTC)));
  }

  private String prepareSortingClause(String sortBy, String order) {
    if (!sortableFields.contains(sortBy)) {
      throw new BadRequestException(format("The specified field for sorting journal records is invalid: '%s'", sortBy));
    }
    return format(ORDER_BY_PATTERN, sortBy, order);
  }

  private JobLogEntryDtoCollection mapRowSetToJobLogDtoCollection(RowSet<Row> rowSet) {
    var jobLogEntryDtoCollection = new JobLogEntryDtoCollection()
      .withTotalRecords(0);

    rowSet.forEach(row ->
      jobLogEntryDtoCollection
        .withTotalRecords(row.getInteger(TOTAL_COUNT))
        .getEntries().add(mapJobLogEntryRow(row))
    );
    return jobLogEntryDtoCollection;
  }

  private JobLogEntryDto mapJobLogEntryRow(Row row) {
    final var entityType = mapToEntityType(row.getString(SOURCE_RECORD_ENTITY_TYPE));
    final var entityHrid = row.getArrayOfStrings(HOLDINGS_ENTITY_HRID);
    final var holdingsActionStatus = mapNameToEntityActionStatus(row.getString(HOLDINGS_ACTION_STATUS));
    return new JobLogEntryDto()
      .withJobExecutionId(row.getValue(JOB_EXECUTION_ID).toString())
      .withSourceRecordId(row.getValue(SOURCE_ID).toString())
      .withSourceRecordOrder(isEmpty(row.getString(INVOICE_ACTION_STATUS))
        ? row.getInteger(SOURCE_RECORD_ORDER).toString()
        : row.getString(INVOICE_LINE_NUMBER))
      .withSourceRecordTitle(getJobLogEntryTitle(row.getString(TITLE), entityType, entityHrid, holdingsActionStatus))
      .withSourceRecordType(entityType)
      .withHoldingsRecordHridList(ArrayUtils.isEmpty(entityHrid) ? Collections.emptyList() : Arrays.asList(entityHrid))
      .withSourceRecordActionStatus(mapNameToEntityActionStatus(row.getString(SOURCE_RECORD_ACTION_STATUS)))
      .withInstanceActionStatus(mapNameToEntityActionStatus(row.getString(INSTANCE_ACTION_STATUS)))
      .withHoldingsActionStatus(holdingsActionStatus)
      .withItemActionStatus(mapNameToEntityActionStatus(row.getString(ITEM_ACTION_STATUS)))
      .withAuthorityActionStatus(mapNameToEntityActionStatus(row.getString(AUTHORITY_ACTION_STATUS)))
      .withPoLineActionStatus(mapNameToEntityActionStatus(row.getString(PO_LINE_ACTION_STATUS)))
      .withInvoiceActionStatus(mapNameToEntityActionStatus(row.getString(INVOICE_ACTION_STATUS)))
      .withInvoiceLineJournalRecordId(Objects.isNull(row.getValue(INVOICE_LINE_JOURNAL_RECORD_ID))
        ? null : row.getValue(INVOICE_LINE_JOURNAL_RECORD_ID).toString())
      .withError(row.getString(ERROR));
  }

  private String getJobLogEntryTitle(String title, JobLogEntryDto.SourceRecordType entityType, String[] entityHrid,
                                     org.folio.rest.jaxrs.model.ActionStatus holdingsActionStatus) {
    return MARC_HOLDINGS.equals(entityType)
      && isActionStatusUpdatedOrCreated(holdingsActionStatus)
      ? "Holdings " + entityHrid[0]
      : title;
  }

  private boolean isActionStatusUpdatedOrCreated(org.folio.rest.jaxrs.model.ActionStatus holdingsActionStatus) {
    return org.folio.rest.jaxrs.model.ActionStatus.CREATED.equals(holdingsActionStatus)
      || org.folio.rest.jaxrs.model.ActionStatus.UPDATED.equals(holdingsActionStatus);
  }

  private RecordProcessingLogDto mapRowSetToRecordProcessingLogDto(RowSet<Row> resultSet) {
    RecordProcessingLogDto recordProcessingLogSummary = new RecordProcessingLogDto();
    if (resultSet.size() == 0) {
      throw new NotFoundException("Can`t find record with specific jobExecutionId and recordId");
    }
    List<ProcessedHoldingsInfo> processedHoldingsInfo = new LinkedList<>();
    List<ProcessedItemInfo> processedItemInfo = new LinkedList<>();

    resultSet.forEach(row -> {
      recordProcessingLogSummary
        .withJobExecutionId(row.getValue(JOB_EXECUTION_ID).toString())
        .withSourceRecordId(row.getValue(SOURCE_ID).toString())
        .withSourceRecordOrder(row.getInteger(SOURCE_RECORD_ORDER))
        .withSourceRecordTitle(row.getString(TITLE))
        .withSourceRecordActionStatus(mapNameToEntityActionStatus(row.getString(SOURCE_RECORD_ACTION_STATUS)))
        .withError(row.getString(SOURCE_ENTITY_ERROR))
        .withRelatedInstanceInfo(constructProcessedEntityWithSingleIdInfoBasedOnEntityType(row,
          INSTANCE_ACTION_STATUS, INSTANCE_ENTITY_ID, INSTANCE_ENTITY_HRID, INSTANCE_ENTITY_ERROR))
        .withRelatedAuthorityInfo(constructProcessedEntityWithSingleIdInfoBasedOnEntityType(row,
          AUTHORITY_ACTION_STATUS, AUTHORITY_ENTITY_ID, null, AUTHORITY_ENTITY_ERROR))
        .withRelatedPoLineInfo(new RelatedPoLineInfo()
          .withActionStatus(mapNameToEntityActionStatus(row.getString(PO_LINE_ACTION_STATUS)))
          .withIdList(constructSingletonListFromColumn(row, PO_LINE_ENTITY_ID))
          .withHridList(constructSingletonListFromColumn(row, PO_LINE_ENTITY_HRID))
          .withError(row.getString(PO_LINE_ENTITY_ERROR))
          .withOrderId(row.getString(ORDER_ENTITY_ID)))
        .withRelatedInvoiceInfo(constructProcessedEntityInfoBasedOnEntityType(row,
          INVOICE_ACTION_STATUS, INVOICE_ENTITY_ID, INVOICE_ENTITY_HRID, INVOICE_ENTITY_ERROR))
        .withRelatedInvoiceLineInfo(constructInvoiceLineInfo(row));
      ProcessedHoldingsInfo processedHoldings = constructProcessedHoldingsInfoBasedOnEntityType(row,HOLDINGS_ACTION_STATUS, HOLDINGS_ENTITY_ID, HOLDINGS_ENTITY_HRID, HOLDINGS_PERMANENT_LOCATION_ID, HOLDINGS_ENTITY_ERROR);
      ProcessedItemInfo processedItem = constructProcessedItemInfoBasedOnEntityType(row, ITEM_ACTION_STATUS, ITEM_ENTITY_ID, ITEM_ENTITY_HRID, HOLDINGS_ENTITY_ID, ITEM_ENTITY_ERROR);
      if(Objects.nonNull(processedHoldings.getId())) {
        processedHoldingsInfo.add(processedHoldings);
      }
      if(Objects.nonNull(processedItem.getId())) {
        processedItemInfo.add(processedItem);
      }
    });
    recordProcessingLogSummary.
      withRelatedItemInfo(processedItemInfo.stream().distinct().collect(Collectors.toList()))
      .withRelatedHoldingsInfo(processedHoldingsInfo.stream().distinct().collect(Collectors.toList()));

    return recordProcessingLogSummary;
  }

  private ProcessedEntityInfo constructProcessedEntityInfoBasedOnEntityType(Row row, String actionStatus, String ids, String hrids, String error) {
    return new ProcessedEntityInfo()
      .withActionStatus(mapNameToEntityActionStatus(row.getString(actionStatus)))
      .withIdList(constructListFromColumn(row, ids))
      .withHridList(constructListFromColumn(row, hrids))
      .withError(row.getString(error));
  }

  private ProcessedEntityInfo constructProcessedEntityWithSingleIdInfoBasedOnEntityType(Row row, String actionStatus, String id, String hrid, String error) {
    return new ProcessedEntityInfo()
      .withActionStatus(mapNameToEntityActionStatus(row.getString(actionStatus)))
      .withIdList(constructSingletonListFromColumn(row,id))
      .withHridList(constructSingletonListFromColumn(row, hrid))
      .withError(row.getString(error));
  }

  private ProcessedHoldingsInfo constructProcessedHoldingsInfoBasedOnEntityType(Row row, String actionStatus, String id, String hrid, String permanentLocationId, String error) {
    return new ProcessedHoldingsInfo()
      .withActionStatus(mapNameToEntityActionStatus(row.getString(actionStatus)))
      .withId(row.getString(id))
      .withHrid(row.getString(hrid))
      .withPermanentLocationId(row.getString(permanentLocationId))
      .withError(row.getString(error));
  }

  private ProcessedItemInfo constructProcessedItemInfoBasedOnEntityType(Row row, String actionStatus, String id, String hrid, String holdingsId, String error) {
    return new ProcessedItemInfo()
      .withActionStatus(mapNameToEntityActionStatus(row.getString(actionStatus)))
      .withId(row.getString(id))
      .withHrid(row.getString(hrid))
      .withHoldingsId(row.getString(holdingsId))
      .withError(row.getString(error));
  }

  private RelatedInvoiceLineInfo constructInvoiceLineInfo(Row row) {
    return new RelatedInvoiceLineInfo()
      .withActionStatus(mapNameToEntityActionStatus(row.getString(INVOICE_LINE_ACTION_STATUS)))
      .withId(row.getValue(INVOICE_LINE_ENTITY_ID) != null ? row.getValue(INVOICE_LINE_ENTITY_ID).toString() : null)
      .withFullInvoiceLineNumber(row.getString(INVOICE_LINE_ENTITY_HRID))
      .withError(row.getString(INVOICE_LINE_ENTITY_ERROR));
  }

  private List<String> constructListFromColumn(Row row, String columnName) {
    return columnName == null || row.getValue(columnName) == null ? Collections.emptyList() : Arrays.asList(row.getArrayOfStrings(columnName));
  }
  private List<String> constructSingletonListFromColumn(Row row, String columnName) {
    return columnName == null || row.getValue(columnName) == null ? Collections.emptyList() : Collections.singletonList(row.getString(columnName));
  }

  private org.folio.rest.jaxrs.model.ActionStatus mapNameToEntityActionStatus(String name) {
    return name == null ? null : org.folio.rest.jaxrs.model.ActionStatus.fromValue(name);
  }

  private JobLogEntryDto.SourceRecordType mapToEntityType(String entityType) {
    return entityType == null ? null : JobLogEntryDto.SourceRecordType.fromValue(entityType);
  }

  private JobExecutionSummaryDto mapRowToJobExecutionSummaryDto(Row row) {
    return new JobExecutionSummaryDto()
      .withJobExecutionId(row.getValue(JOB_EXECUTION_ID).toString())
      .withTotalErrors(row.getInteger(TOTAL_ERRORS))
      .withSourceRecordSummary(mapToEntityProcessingSummary(row, TOTAL_CREATED_SOURCE_RECORDS, TOTAL_UPDATED_SOURCE_RECORDS,
        TOTAL_DISCARDED_SOURCE_RECORDS, TOTAL_SOURCE_RECORDS_ERRORS))
      .withInstanceSummary(mapToEntityProcessingSummary(row, TOTAL_CREATED_INSTANCES, TOTAL_UPDATED_INSTANCES,
        TOTAL_DISCARDED_INSTANCES, TOTAL_INSTANCES_ERRORS))
      .withHoldingSummary(mapToEntityProcessingSummary(row, TOTAL_CREATED_HOLDINGS, TOTAL_UPDATED_HOLDINGS,
        TOTAL_DISCARDED_HOLDINGS, TOTAL_HOLDINGS_ERRORS))
      .withItemSummary(mapToEntityProcessingSummary(row, TOTAL_CREATED_ITEMS, TOTAL_UPDATED_ITEMS,
        TOTAL_DISCARDED_ITEMS, TOTAL_ITEMS_ERRORS))
      .withAuthoritySummary(mapToEntityProcessingSummary(row, TOTAL_CREATED_AUTHORITIES, TOTAL_UPDATED_AUTHORITIES,
        TOTAL_DISCARDED_AUTHORITIES, TOTAL_AUTHORITIES_ERRORS))
      .withInvoiceSummary(mapToEntityProcessingSummary(row, TOTAL_CREATED_INVOICES, TOTAL_UPDATED_INVOICES,
        TOTAL_DISCARDED_INVOICES, TOTAL_INVOICES_ERRORS))
      .withOrderSummary(mapToEntityProcessingSummary(row, TOTAL_CREATED_ORDERS, TOTAL_UPDATED_ORDERS,
        TOTAL_DISCARDED_ORDERS, TOTAL_ORDERS_ERRORS));
  }

  private EntityProcessingSummary mapToEntityProcessingSummary(Row row, String totalCreatedColumn, String totalUpdatedColumn,
                                                               String totalDiscardedColumn, String totalErrorsColumn) {
    Integer totalCreated = row.getInteger(totalCreatedColumn);
    Integer totalUpdated = row.getInteger(totalUpdatedColumn);
    Integer totalDiscarded = row.getInteger(totalDiscardedColumn);
    Integer totalErrors = row.getInteger(totalErrorsColumn);

    if (0 == totalCreated && 0 == totalUpdated && 0 == totalDiscarded && 0 == totalErrors) {
      return null;
    }

    return new EntityProcessingSummary()
      .withTotalCreatedEntities(totalCreated)
      .withTotalUpdatedEntities(totalUpdated)
      .withTotalDiscardedEntities(totalDiscarded)
      .withTotalErrors(totalErrors);
  }
}
