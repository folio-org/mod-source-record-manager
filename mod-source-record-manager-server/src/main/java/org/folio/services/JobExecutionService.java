package org.folio.services;

import io.vertx.core.Future;
import org.folio.dao.JobExecutionDao;
import org.folio.dao.JobExecutionFilter;
import org.folio.dao.util.SortField;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.DeleteJobExecutionsResp;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDtoCollection;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.JobProfileInfoCollection;
import org.folio.rest.jaxrs.model.JobExecutionUserInfoCollection;

import java.util.List;
import java.util.Optional;

/**
 * JobExecution Service interface, contains logic for accessing jobs.
 *
 * @see JobExecution
 * @see org.folio.dao.JobExecutionDao
 * @see org.folio.rest.jaxrs.model.JobExecutionDtoCollection
 */
public interface JobExecutionService {

  /**
   * Returns JobExecutionCollectionDto by the input filter
   *
   * @param filter filter containing conditions by which jobExecutions should be filtered
   * @param sortFields fields to sort jobExecutions
   * @param offset starting index in a list of results
   * @param limit  maximum number of results to return
   * @return future with JobExecutionCollectionDto
   */
  Future<JobExecutionDtoCollection> getJobExecutionsWithoutParentMultiple(JobExecutionFilter filter, List<SortField> sortFields, int offset, int limit, String tenantId);

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
   * Updates jobExecution and calls source-record-storage to update Snapshot status by asynchronous way
   *
   * @param jobExecution entity to update
   * @param params       connection parameters
   * @return updated entity
   */
  Future<JobExecution> updateJobExecutionWithSnapshotStatusAsync(JobExecution jobExecution, OkapiConnectionParams params);

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
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @return future with collection of child JobExecutions
   */
  Future<JobExecutionDtoCollection> getJobExecutionCollectionByParentId(String parentId, int offset, int limit, String tenantId);

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
   * Sets JobProfile and JobProfile snapshot wrapper for JobExecution
   *
   * @param jobExecutionId JobExecution id
   * @param jobProfile     JobProfileInfo entity
   * @param params         connection parameters
   * @return future with updated JobExecution
   */
  Future<JobExecution> setJobProfileToJobExecution(String jobExecutionId, JobProfileInfo jobProfile, OkapiConnectionParams params);

  /**
   * Sets JobExecution status to ERROR and deletes all associated records from SRS
   *
   * @param jobExecutionId JobExecution id
   * @param params         connection parameters
   * @return future with true if succeeded
   */
  Future<Boolean> completeJobExecutionWithError(String jobExecutionId, OkapiConnectionParams params);


  /**
   *
   * @param ids JobExecutions to be deleted using Ids
   * @param tenantId
   * @return future of boolean depending upon success and failure
   */
  Future<DeleteJobExecutionsResp>  softDeleteJobExecutionsByIds(List<String> ids, String tenantId);

  /**
   * Searches for JobProfilesInfo,
   * by default returns all existing related job profiles,
   * to limit the collection param limit should be explicitly specified
   *
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @return future with collection of JobProfileInfo
   */
  Future<JobProfileInfoCollection> getRelatedJobProfiles(int offset, int limit, String tenantId);

  /**
   * Searches unique users for jobExecutions. Returns list of unique users
   * with "userId", "firstName", "lastName".
   *
   * @param offset   starting index in a list of results
   * @param limit    maximum number of results to return
   * @return collection of JobExecutionUserInfoCollection with userIds, firstNames, lastNames
   */
  Future<JobExecutionUserInfoCollection> getRelatedUsersInfo(int offset, int limit, String tenantId);
}
