package org.folio.services;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollectionDto;

/**
 * JobExecution Service interface, contains logic for accessing jobs.
 * @see JobExecution
 * @see org.folio.dao.JobExecutionDao
 * @see org.folio.rest.jaxrs.model.JobExecutionDto
 */
public interface JobExecutionService {

  /**
   * Returns JobExecutionCollectionDto by the input query
   *
   * @param query  query string to filter entities
   * @param offset starting index in a list of results
   * @param limit  maximum number of results to return
   */
  Future<JobExecutionCollectionDto> getCollectionDtoByQuery(String query, int offset, int limit);

}
