package org.folio.dao;

import io.vertx.core.Future;
import org.folio.dao.util.JobExecutionMutator;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollectionDto;

import java.util.Optional;

/**
 * DAO interface for the JobExecution entity
 *
 * @see JobExecution
 */
public interface JobExecutionDao {

  /**
   * Searches for {@link JobExecution} in the db which do not have subordinationType=PARENT_MULTIPLE
   * (only CHILD and PARENT_SINGLE allowed).
   *
   * @param query  query string to filter jobExecutions based on matching criteria in fields
   * @param offset starting index in a list of results
   * @param limit  maximum number of results to return
   * @return future with {@link org.folio.rest.jaxrs.model.JobExecutionCollectionDto}
   */
  Future<JobExecutionCollectionDto> getJobExecutionsWithoutParentMultiple(String query, int offset, int limit, String tenantId);

  /**
   * Saves {@link JobExecution} to database
   *
   * @param jobExecution {@link JobExecution} to save
   * @return future
   */
  Future<String> save(JobExecution jobExecution, String tenantId);

  /**
   * Updates {@link JobExecution}
   *
   * @param jobExecution entity to update
   * @return updated entity
   */
  Future<JobExecution> updateJobExecution(JobExecution jobExecution, String tenantId);

  /**
   * Searches for {@link JobExecution} by parent id
   *
   * @param parentId parent id
   * @param query    query string to filter jobExecutions based on matching criteria in fields
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @return collection of JobExecutionCollection dtos with specified parent id
   */
  Future<JobExecutionCollectionDto> getChildrenJobExecutionsByParentId(String parentId, String query, int offset, int limit, String tenantId);

  /**
   * Searches for {@link JobExecution} by id
   *
   * @param id jobExecution id
   * @return optional of JobExecution
   */
  Future<Optional<JobExecution>> getJobExecutionById(String id, String tenantId);

  /**
   * Updates {@link JobExecution} in the db with row blocking
   *
   * @param jobExecutionId JobExecution id
   * @param mutator        defines necessary changes to be made before save
   * @return future with updated JobExecution
   */
  Future<JobExecution> updateBlocking(String jobExecutionId, JobExecutionMutator mutator, String tenantId);

  /**
   * Deletes {@link JobExecution} by id
   *
   * @param jobExecutionId jobExecution id
   * @param tenantId       tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> deleteJobExecutionById(String jobExecutionId, String tenantId);

}
