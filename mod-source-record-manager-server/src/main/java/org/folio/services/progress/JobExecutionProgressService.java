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
   * @param jobExecutionId  jobExecution id
   * @param tenantId        tenant id
   * @return future with jobExecutionProgress
   */
  Future<JobExecutionProgress> getByJobExecutionId(String jobExecutionId, String tenantId);

  /**
   * Creates jobExecutionProgress for {@link JobExecution} with specified jobExecutionId
   *
   * @param jobExecutionId  jobExecution id
   * @param totalRecords    total number of records to be processed
   * @param tenantId        tenant id
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
}
