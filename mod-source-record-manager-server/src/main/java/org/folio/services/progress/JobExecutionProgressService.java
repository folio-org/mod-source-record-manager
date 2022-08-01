package org.folio.services.progress;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionProgress;

import java.util.function.UnaryOperator;

/**
 * JobExecutionProgress service interface
 */
public interface JobExecutionProgressService {

  /**
   * Searches for jobExecutionProgress by {@link JobExecution} id
   *
   * @param jobExecutionId jobExecution id
   * @param tenantId       tenant id
   * @return future with jobExecutionProgress
   */
  Future<JobExecutionProgress> getByJobExecutionId(String jobExecutionId, String tenantId);

  /**
   * Creates jobExecutionProgress for {@link JobExecution} with specified jobExecutionId
   *
   * @param jobExecutionId jobExecution id
   * @param totalRecords   total number of records to be processed
   * @param tenantId       tenant id
   * @return future with created JobExecutionProgress
   */
  Future<JobExecutionProgress> initializeJobExecutionProgress(String jobExecutionId, Integer totalRecords, String tenantId);

  /**
   * Updates jobExecutionProgress entity by jobExecutionId
   *
   * @param jobExecutionId  jobExecution id
   * @param progressMutator defines changes to be made on entity for update
   * @param tenantId        tenant id
   * @return future with updated jobExecutionProgress
   */
  Future<JobExecutionProgress> updateJobExecutionProgress(String jobExecutionId, UnaryOperator<JobExecutionProgress> progressMutator, String tenantId);

  /**
   * Updates jobExecutionProgress entity by adding deltas to the number of success and error counts.
   * <p>
   * EXAMPLE:
   *  a success count of 5 and an error count of 0 means that the success count will be increased by 5 and the
   *  error count will remain unchanged in jobExecutionProgress entity.
   *
   *
   * @param jobExecutionId  jobExecution id
   * @param successCountDelta number of successful executions
   * @param errorCountDelta number of failed executions
   * @param tenantId        tenant id
   * @return future with updated jobExecutionProgress
   */
  Future<JobExecutionProgress> updateCompletionCounts(String jobExecutionId, int successCountDelta, int errorCountDelta, String tenantId);
}
