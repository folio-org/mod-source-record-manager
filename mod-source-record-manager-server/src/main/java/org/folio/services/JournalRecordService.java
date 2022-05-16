package org.folio.services;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.JobExecutionSummaryDto;
import org.folio.rest.jaxrs.model.JobLogEntryDtoCollection;
import org.folio.rest.jaxrs.model.JournalRecordCollection;
import org.folio.rest.jaxrs.model.RecordProcessingLogDto;

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
   * @param limit          limit
   * @param offset         offset
   * @param tenantId       tenantId
   * @return future with JobLogEntryDto collection
   */
  Future<JobLogEntryDtoCollection> getJobLogEntryDtoCollection(String jobExecutionId, String sortBy, String order, int limit, int offset, String tenantId);

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
}
