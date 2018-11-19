package org.folio.services;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollectionDto;

import java.util.List;
import java.util.Optional;

/**
 * JobExecution Service interface, contains logic for accessing jobs.
 * @see JobExecution
 * @see org.folio.dao.JobExecutionDao
 * @see org.folio.rest.jaxrs.model.JobExecutionDto
 */
public interface JobExecutionService {

  /**
   * Returns List of JobExecution entities by the input query
   *
   * @param query  query string to filter entities
   * @param offset starting index in a list of results
   * @param limit  maximum number of results to return
   */
  Future<List<JobExecution>> getByQuery(String query, int offset, int limit);

  /**
   * Returns JobExecutionCollectionDto by the input query
   *
   * @param query  query string to filter entities
   * @param offset starting index in a list of results
   * @param limit  maximum number of results to return
   */
  Future<JobExecutionCollectionDto> getCollectionDtoByQuery(String query, int offset, int limit);

  /**
   * Retrieves JobExecution if it exists
   *
   * @param id UUID value
   * @return optional entity
   */
  Future<Optional<JobExecution>> getById(String id);


  /**
   * Saves JobExecution into the storage and returns it
   *
   * @param jobExecution entity to save
   * @return entity
   */
  Future<JobExecution> save(JobExecution jobExecution);

  /**
   * Updates JobExecution into the storage and returns it
   *
   * @param jobExecution JobExecution entity to update
   * @return entity
   */
  Future<JobExecution> update(JobExecution jobExecution);

  /**
   * Removes JobExecution entity from the storage
   *
   * @param id UUID value
   * @return boolean result, true if the entity has been successfully removed
   */
  Future<Boolean> delete(String id);
}
