package org.folio.dao;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionProgress;

import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * DAO interface for the JobExecutionProgress entity
 */
public interface JobExecutionProgressDao {

  /**
   * Searches for jobExecutionProgress by {@link JobExecution} id
   *
   * @param jobExectuionId jobExectuion id
   * @param tenantId       tenant id
   * @return future with jobExecutionProgress
   */
  Future<Optional<JobExecutionProgress>> getByJobExecutionId(String jobExectuionId, String tenantId);

  /**
   * Saves jobExecutionProgress entity to database
   *
   * @param progress entity to save
   * @param tenantId tenant id
   * @return future
   */
  Future<String> save(JobExecutionProgress progress, String tenantId);

  /**
   * Updates jobExecutionProgress entity by jobExecutionId in database
   *
   * @param jobExecutionId  jobExecution id
   * @param progressMutator defines changes on entity for update
   * @param tenantId        tenant id
   * @return future with updated jobExecutionProgress
   */
  Future<JobExecutionProgress> updateByJobExecutionId(String jobExecutionId, UnaryOperator<JobExecutionProgress> progressMutator, String tenantId);
}
