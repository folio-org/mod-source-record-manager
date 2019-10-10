package org.folio.services;

import io.vertx.core.Future;
import org.folio.dao.JobExecutionDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.jaxrs.model.JobExecutionCollectionDto;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.LogCollectionDto;
import org.folio.rest.jaxrs.model.StatusDto;

import java.util.Optional;

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
  Future<JobExecutionCollectionDto> getJobExecutionCollectionDtoByQuery(String query, int offset, int limit, String tenantId);

  /**
   * Returns LogCollectionDto by the input query
   *
   * @param query  query string to filter entities
   * @param offset starting index in a list of results
   * @param limit  maximum number of results to return
   */
  Future<LogCollectionDto> getLogCollectionDtoByQuery(String query, int offset, int limit, String tenantId);

  /**
   * Performs creation of JobExecution and Snapshot entities
   * Saves created JobExecution entities into storage using {@link JobExecutionDao}
   * Performs save for created Snapshot entities.
   * For each Snapshot posts the request to source-record-storage.
   *
   * @param dto    Dto contains request params enough to create JobExecution and Snapshot entities
   * @param params object-wrapper with params necessary to connect to OKAPI
   * @return Future
   */
  Future<InitJobExecutionsRsDto> initializeJobExecutions(InitJobExecutionsRqDto dto, OkapiConnectionParams params);

  /**
   * Updates jobExecution and calls source-record-storage to update Snapshot status
   *
   * @param jobExecution entity to update
   * @param params       connection parameters
   * @return updated entity
   */
  Future<JobExecution> updateJobExecutionWithSnapshotStatus(JobExecution jobExecution, OkapiConnectionParams params);

  /**
   * Updates jobExecution
   *
   * @param jobExecution entity to update
   * @param params       connection parameters
   * @return updated entity
   */
  Future<JobExecution> updateJobExecution(JobExecution jobExecution, OkapiConnectionParams params);

  /**
   * Searches for JobExecution by id
   *
   * @param id JobExecution id
   * @return future with optional JobExecution
   */
  Future<Optional<JobExecution>> getJobExecutionById(String id, String tenantId);

  /**
   * Searches for children JobExecutions by parent id,
   * by default returns all existing children JobExecutions,
   * to limit the collection param limit should be explicitly specified
   *
   * @param parentId JobExecution parent id
   * @param query    query string to filter entities
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @return future with collection of child JobExecutions
   */
  Future<JobExecutionCollection> getJobExecutionCollectionByParentId(String parentId, String query, int offset, int limit, String tenantId);

  /**
   * Updates status for JobExecution and calls source-record-storage to update Snapshot status
   *
   * @param jobExecutionId JobExecution id
   * @param status         Dto that contains new status
   * @param params         connection parameters
   * @return future with updated JobExecution
   */
  Future<JobExecution> updateJobExecutionStatus(String jobExecutionId, StatusDto status, OkapiConnectionParams params);

  /**
   * Sets JobProfile for JobExecution
   *
   * @param jobExecutionId JobExecution id
   * @param jobProfile     JobProfileInfo entity
   * @return future with updated JobExecution
   */
  Future<JobExecution> setJobProfileToJobExecution(String jobExecutionId, JobProfileInfo jobProfile, String tenantId);

  /**
   * Delete JobExecution and all associated records from SRS
   *
   * @param jobExecutionId JobExecution id
   * @param params         connection parameters
   * @return future with true if succeeded
   */
  Future<Boolean> deleteJobExecutionAndSRSRecords(String jobExecutionId, OkapiConnectionParams params);

}
