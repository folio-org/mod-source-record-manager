package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.folio.rest.jaxrs.model.JobExecutionSummaryDto;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.RecordProcessingLogDto;
import org.folio.rest.jaxrs.model.RecordProcessingLogDtoCollection;

import java.util.List;
import java.util.Optional;

/**
 * DAO interface for the JournalRecord entity
 */
public interface JournalRecordDao {

  /**
   * Saves JournalRecord entity to DB
   *
   * @param journalRecord journalRecord entity to save
   * @param tenantId      tenant id
   * @return future with created journalRecord id
   */
  Future<String> save(JournalRecord journalRecord, String tenantId);

  /**
   * Saves chunk of JournalRecord entities to DB
   *
   * @param journalRecords  List of JournalRecord entities to save
   * @param tenantId        tenant id
   * @return future with created JournalRecord entities
   */
  Future<List<RowSet<Row>>> saveBatch(List<JournalRecord> journalRecords, String tenantId);

  /**
   * Searches for JournalRecord entities by jobExecutionId and sorts them using specified sort criteria and direction
   *
   * @param jobExecutionId job execution id
   * @param sortBy         sort criteria
   * @param order          sort direction
   * @param tenantId       tenant id
   * @return future with list of journalRecord entities
   */
  Future<List<JournalRecord>> getByJobExecutionId(String jobExecutionId, String sortBy, String order, String tenantId);

  /**
   * Deletes journal records associated with job execution by specified jobExecutionId
   *
   * @param jobExecutionId job execution id
   * @param tenantId       tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> deleteByJobExecutionId(String jobExecutionId, String tenantId);

  /**
   * Searches for RecordProcessingLogDtoCollection by jobExecutionId and sorts them using specified sort criteria and direction
   *
   * @param jobExecutionId job execution id
   * @param sortBy         sorting criteria
   * @param order          sorting direction
   * @param errorsOnly     filtering by error field
   * @param entityType     filtering by entity type
   * @param limit          limit
   * @param offset         offset
   * @param tenantId       tenantId
   * @return future with JobLogEntryDto collection
   */
  Future<RecordProcessingLogDtoCollection> getRecordProcessingLogDtoCollection(String jobExecutionId, String sortBy, String order, boolean errorsOnly, String entityType, int limit, int offset, String tenantId);

  /**
   * Searches for RecordProcessingLogDto entities by jobExecutionId and recordId
   *
   * @param jobExecutionId job execution id
   * @param recordId       record id
   * @param tenantId       tenant id
   * @return future with RecordProcessingLogDto collection
   */
  Future<RecordProcessingLogDto> getRecordProcessingLogDto(String jobExecutionId, String recordId, String tenantId);

  /**
   * Returns JobExecutionSummaryDto for job execution by {@code jobExecutionId}
   *
   * @param jobExecutionId job execution id
   * @param tenantId       tenantId
   * @return future with {@link Optional} of JobExecutionSummaryDto
   */
  Future<Optional<JobExecutionSummaryDto>> getJobExecutionSummaryDto(String jobExecutionId, String tenantId);

  /**
   * Updates JournalRecords error-field by orderId and jobExecutionId
   * @param jobExecutionId job execution id
   * @param orderId orderId
   * @param error current error message which should be set
   * @param tenantId tenantId
   * @return future with number of updated JournalRecords
   */
  Future<Integer> updateErrorJournalRecordsByOrderIdAndJobExecution(String jobExecutionId, String orderId, String error, String tenantId);
}
