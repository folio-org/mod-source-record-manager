package org.folio.services;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.JobExecutionSummaryDto;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.JobLogEntryDtoCollection;
import org.folio.rest.jaxrs.model.JournalRecordCollection;
import org.folio.rest.jaxrs.model.RecordProcessingLogDto;

import java.util.List;
import java.util.Optional;

/**
 * JournalRecord Service interface.
 */
public interface JournalRecordService {

  /**
   * Deletes journal records associated with job execution by specified id
   *
   * @param jobExecutionId job execution id
   * @param tenantId       tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> deleteByJobExecutionId(String jobExecutionId, String tenantId);

  /**
   * Searches for JournalRecords by jobExecutionId and sorts them using specified sort criteria and direction
   *
   * @param jobExecutionId job execution id
   * @param sortBy         sort criteria
   * @param order          sort direction
   * @param tenantId       tenant id
   * @return future with JournalRecordCollection
   */
  Future<JournalRecordCollection> getJobExecutionJournalRecords(String jobExecutionId, String sortBy, String order, String tenantId);

  /**
   * Searches for JobLogEntryDto entities by jobExecutionId and sorts them using specified sort criteria and direction
   *
   * @param jobExecutionId job execution id
   * @param sortBy         sorting criteria
   * @param order          sorting direction
   * @param errorsOnly     filtering by error field
   * @param errorsOnly     filtering by entity type
   * @param limit          limit
   * @param offset         offset
   * @param tenantId       tenantId
   * @return future with JobLogEntryDto collection
   */
  Future<JobLogEntryDtoCollection> getJobLogEntryDtoCollection(String jobExecutionId, String sortBy, String order, boolean errorsOnly, String entityType, int limit, int offset, String tenantId);

  /**
   * Searches for RecordProcessingLogDto entity by jobExecutionId and recordId
   * @param jobExecutionId - job execution id
   * @param recordId - record id
   * @param tenantId - tenant id
   * @return future with RecordProcessingLogDto
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
   * Updates  JournalRecords error-field with current error-message by the same orderId and jobExecutionId
   * @param jobExecutionId jobExecutionId
   * @param orderId orderId
   * @param error error
   * @param tenantId tenantId
   * @return Future with JournalRecords updated number
   */
  Future<Integer> updateErrorJournalRecordsByOrderIdAndJobExecution(String jobExecutionId, String orderId, String error, String tenantId);

  /**
   * Saves set of {@link JournalRecord} entities
   *
   * @param journalRecords journal records to save
   * @param tenantId       tenant id
   */
  void saveBatch(List<JournalRecord> journalRecords, String tenantId);
}
