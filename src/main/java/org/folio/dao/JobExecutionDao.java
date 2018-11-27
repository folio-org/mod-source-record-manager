package org.folio.dao;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;

import java.util.List;
import java.util.Optional;

/**
 * DAO interface for the JobExecution entity
 *
 * @see JobExecution
 */
public interface JobExecutionDao {

  /**
   * Searches for {@link JobExecution} in the db
   *
   * @param query  query string to filter jobExecutions based on matching criteria in fields
   * @param offset starting index in a list of results
   * @param limit  maximum number of results to return
   * @return future with {@link JobExecutionCollection}
   */
  Future<JobExecutionCollection> getJobExecutions(String query, int offset, int limit);

  /**
   * Saves {@link JobExecution} to database
   *
   * @param jobExecution {@link JobExecution} to save
   * @return future
   */
  Future<String> save(JobExecution jobExecution);

  /**
   * Updates {@link JobExecution}
   *
   * @param jobExecution entity to update
   * @return updated entity
   */
  Future<JobExecution> updateJobExecution(JobExecution jobExecution);

  /**
   * Searches for {@link JobExecution} by parent id
   *
   * @param parentId parent id
   * @return list of JobExecutions with specified parent id
   */
  Future<List<JobExecution>> getJobExecutionsByParentId(String parentId);

  /**
   * Searches for {@link JobExecution} by id
   *
   * @param id jobExecution id
   * @return optional of JobExecution
   */
  Future<Optional<JobExecution>> getJobExecutionById(String id);

}
