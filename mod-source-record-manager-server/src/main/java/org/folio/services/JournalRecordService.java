package org.folio.services;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.JobExecutionLogDto;
import org.folio.rest.jaxrs.model.JobLogEntryDtoCollection;
import org.folio.rest.jaxrs.model.JournalRecordCollection;

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
   * Returns JobExecutionLogDto with import results for job execution with specified id
   *
   * @param jobExecutionId jobExecution id
   * @param tenantId       tenant id
   * @return future with JobExecutionLogDto entity
   */
  Future<JobExecutionLogDto> getJobExecutionLogDto(String jobExecutionId, String tenantId);

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
}
