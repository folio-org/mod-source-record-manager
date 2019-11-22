package org.folio.services;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.JobExecutionLogDto;

/**
 * JournalRecord Service interface.
 */
public interface JournalRecordService {

  /**
   * Deletes journal records associated with job execution by specified id
   *
   * @param jobExecutionId job execution id
   * @param tenantId tenant id
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
}
