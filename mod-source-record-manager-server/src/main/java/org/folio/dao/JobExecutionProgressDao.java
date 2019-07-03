package org.folio.dao;

import io.vertx.core.Future;
import org.folio.dao.util.JobExecutionProgressMutator;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.Progress;

import java.util.Optional;

/**
 * DAO interface for the JobExecution's progress entity
 *
 * @see Progress
 * @see JobExecution
 */
public interface JobExecutionProgressDao {

  /**
   * Saves {@link Progress} to database
   *
   * @param progress {@link Progress} to save
   * @return future
   */
  Future<String> save(Progress progress, String tenantId);

  /**
   * Updates {@link Progress} in the db with row blocking
   *
   * @param jobExecutionId JobExecution id
   * @param mutator        defines necessary changes to be made before save
   * @return future with updated Progress
   */
  Future<Progress> updateBlocking(String jobExecutionId, JobExecutionProgressMutator mutator, String tenantId);

  /**
   * Searches for {@link Progress} by id of jobExecution
   *
   * @param jobExecutionId jobExecution id
   * @return optional of JobExecution
   */
  Future<Optional<Progress>> getProgressByJobExecutionId(String jobExecutionId, String tenantId);
}
