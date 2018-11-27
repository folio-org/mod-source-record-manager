package org.folio.services;

import io.vertx.core.Future;
import org.folio.dao.JobExecutionDao;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollectionDto;
import org.folio.util.OkapiConnectionParams;

/**
 * JobExecution Service interface, contains logic for accessing jobs.
 *
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

  /**
   * Performs creation of JobExecution and Snapshot entities
   * Saves created JobExecution entities into storage using {@link JobExecutionDao}
   * Performs save for created Snapshot entities.
   * For each Snapshot posts the request to mod-source-record-manager.
   *
   * @param dto    Dto contains request params enough to create JobExecution and Snapshot entities
   * @param params object-wrapper with params necessary to connect to OKAPI
   * @return Future
   */
  Future<InitJobExecutionsRsDto> initializeJobExecutions(InitJobExecutionsRqDto dto, OkapiConnectionParams params);

  /**
   * Updates jobExecution and its children in case it is a PARENT_MULTIPLE jobExecution
   *
   * @param jobExecution entity to update
   * @return updated entity
   */
  Future<JobExecution> updateJobExecution(JobExecution jobExecution);

}
