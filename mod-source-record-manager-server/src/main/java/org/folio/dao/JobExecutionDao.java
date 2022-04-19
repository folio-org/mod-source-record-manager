package org.folio.dao;

import io.vertx.core.Future;
import org.folio.dao.util.JobExecutionMutator;
import org.folio.dao.util.SortField;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDtoCollection;

import java.util.List;
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
   * @param filter filter containing conditions by which jobExecutions should be filtered
   * @param sortFields fields to sort jobExecutions
   * @param offset starting index in a list of results
   * @param limit  maximum number of results to return
   * @return future with {@link org.folio.rest.jaxrs.model.JobExecutionDtoCollection}
   */
  Future<JobExecutionDtoCollection> getJobExecutionsWithoutParentMultiple(JobExecutionFilter filter, List<SortField> sortFields, int offset, int limit, String tenantId);

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
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @return collection of JobExecutionCollection dtos with specified parent id
   */
  Future<JobExecutionDtoCollection> getChildrenJobExecutionsByParentId(String parentId, int offset, int limit, String tenantId);

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

  Future<Boolean> deleteJobExecutionByIds(List<String> ids, String tenantId);
}
