package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.HttpException;

import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.HttpStatus;
import org.folio.dao.JobExecutionDao;
import org.folio.dao.JobExecutionFilter;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dao.util.SortField;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.dataimport.util.Try;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.rest.client.DataImportProfilesClient;
import org.folio.rest.client.SourceStorageSnapshotsClient;
import org.folio.rest.jaxrs.model.DeleteJobExecutionsResp;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDtoCollection;
import org.folio.rest.jaxrs.model.JobExecutionUserInfoCollection;
import org.folio.rest.jaxrs.model.JobProfile;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Progress;
import org.folio.rest.jaxrs.model.RunBy;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.UserInfo;
import org.folio.rest.jaxrs.model.JobProfileInfoCollection;
import org.folio.services.exceptions.JobDuplicateUpdateException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

import static java.lang.String.format;
import static org.folio.HttpStatus.HTTP_CREATED;
import static org.folio.HttpStatus.HTTP_OK;
import static org.folio.rest.jaxrs.model.JobExecution.Status.COMMITTED;
import static org.folio.rest.jaxrs.model.StatusDto.ErrorStatus.PROFILE_SNAPSHOT_CREATING_ERROR;
import static org.folio.rest.jaxrs.model.StatusDto.Status.CANCELLED;
import static org.folio.rest.jaxrs.model.StatusDto.Status.ERROR;

/**
 * Implementation of the JobExecutionService, calls JobExecutionDao to access JobExecution metadata.
 *
 * @see JobExecutionService
 * @see JobExecutionDao
 * @see JobExecution
 */
@Service
public class JobExecutionServiceImpl implements JobExecutionService {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final String GET_USER_URL = "/users?query=id==";
  private static final String DEFAULT_LASTNAME = "SYSTEM";
  private static final String DEFAULT_JOB_PROFILE = "CLI Create MARC Bibs and Instances";
  private static final String DEFAULT_JOB_PROFILE_ID = "22fafcc3-f582-493d-88b0-3c538480cd83";
  private static final String NO_FILE_NAME = "No file name";

  @Autowired
  private JobExecutionDao jobExecutionDao;
  @Autowired
  private JobExecutionSourceChunkDao jobExecutionSourceChunkDao;
  @Autowired
  private JournalRecordService journalRecordService;

  @Override
  public Future<JobExecutionDtoCollection> getJobExecutionsWithoutParentMultiple(JobExecutionFilter filter, List<SortField> sortFields, int offset, int limit, String tenantId) {
    return jobExecutionDao.getJobExecutionsWithoutParentMultiple(filter, sortFields, offset, limit, tenantId);
  }

  @Override
  public Future<InitJobExecutionsRsDto> initializeJobExecutions(InitJobExecutionsRqDto jobExecutionsRqDto, OkapiConnectionParams params) {
    if (jobExecutionsRqDto.getSourceType().equals(InitJobExecutionsRqDto.SourceType.FILES) && jobExecutionsRqDto.getFiles().isEmpty()) {
      String errorMessage = "Received files must not be empty";
      LOGGER.error(errorMessage);
      return Future.failedFuture(new BadRequestException(errorMessage));
    } else {
      String parentJobExecutionId = UUID.randomUUID().toString();
      return lookupUser(jobExecutionsRqDto.getUserId(), params)
        .compose(userInfo -> {
          List<JobExecution> jobExecutions =
            prepareJobExecutionList(parentJobExecutionId, jobExecutionsRqDto.getFiles(), userInfo, jobExecutionsRqDto);
          List<Snapshot> snapshots = prepareSnapshotList(jobExecutions);
          Future<List<String>> savedJsonExecutionsFuture = saveJobExecutions(jobExecutions, params.getTenantId());
          Future<List<String>> savedSnapshotsFuture = saveSnapshots(snapshots, params);
          return GenericCompositeFuture.all(Arrays.asList(savedJsonExecutionsFuture, savedSnapshotsFuture))
            .map(new InitJobExecutionsRsDto()
              .withParentJobExecutionId(parentJobExecutionId)
              .withJobExecutions(jobExecutions));
        });
    }
  }

  @Override
  public Future<JobExecution> updateJobExecutionWithSnapshotStatus(JobExecution jobExecution, OkapiConnectionParams params) {
    return updateJobExecution(jobExecution, params)
      .compose(jobExec -> updateSnapshotStatus(jobExecution, params));
  }

  @Override
  public Future<JobExecution> updateJobExecution(JobExecution jobExecution, OkapiConnectionParams params) {
    return jobExecutionDao.updateBlocking(jobExecution.getId(), currentJobExec -> {
      Promise<JobExecution> promise = Promise.promise();
      if (JobExecution.Status.PARENT.equals(jobExecution.getStatus()) ^ JobExecution.Status.PARENT.equals(currentJobExec.getStatus())) {
        String errorMessage = format("JobExecution %s current status is %s and cannot be updated to %s",
          currentJobExec.getId(), currentJobExec.getStatus(), jobExecution.getStatus());
        LOGGER.error(errorMessage);
        promise.fail(new BadRequestException(errorMessage));
      } else {
        currentJobExec = jobExecution;
        promise.complete(currentJobExec);
      }
      return promise.future();
    }, params.getTenantId());
  }

  @Override
  public Future<Optional<JobExecution>> getJobExecutionById(String id, String tenantId) {
    return jobExecutionDao.getJobExecutionById(id, tenantId);
  }

  @Override
  public Future<JobExecutionDtoCollection> getJobExecutionCollectionByParentId(String parentId, int offset, int limit, String tenantId) {
    return jobExecutionDao.getJobExecutionById(parentId, tenantId)
      .compose(optionalJobExecution -> optionalJobExecution
        .map(jobExec -> {
          if (JobExecution.SubordinationType.PARENT_MULTIPLE.equals(jobExec.getSubordinationType())) {
            return jobExecutionDao.getChildrenJobExecutionsByParentId(jobExec.getId(), offset, limit, tenantId);
          } else {
            return Future.succeededFuture(new JobExecutionDtoCollection().withTotalRecords(0));
          }
        })
        .orElse(Future.failedFuture(new NotFoundException(
          format("JobExecution with id '%s' was not found", parentId))))
      );
  }

  @Override
  public Future<JobExecution> updateJobExecutionStatus(String jobExecutionId, StatusDto status, OkapiConnectionParams params) {
    if (JobExecution.Status.PARENT.name().equals(status.getStatus().name())) {
      String errorMessage = "Cannot update JobExecution status to PARENT";
      LOGGER.error(errorMessage);
      return Future.failedFuture(new BadRequestException(errorMessage));
    } else {
      return jobExecutionDao.updateBlocking(jobExecutionId, jobExecution -> {
        Promise<JobExecution> promise = Promise.promise();
        try {
          if (JobExecution.Status.PARENT.name().equals(jobExecution.getStatus().name())) {
            String message = format("JobExecution %s current status is PARENT and cannot be updated", jobExecutionId);
            LOGGER.error(message);
            promise.fail(new BadRequestException(message));
          } else {
            jobExecution.setStatus(JobExecution.Status.fromValue(status.getStatus().name()));
            jobExecution.setUiStatus(JobExecution.UiStatus.fromValue(Status.valueOf(status.getStatus().name()).getUiStatus()));
            updateJobExecutionIfErrorExist(status, jobExecution);
            promise.complete(jobExecution);
          }
        } catch (Exception e) {
          String errorMessage = "Error updating JobExecution with id " + jobExecutionId;
          LOGGER.error(errorMessage, e);
          promise.fail(errorMessage);
        }
        return promise.future();
      }, params.getTenantId())
        .compose(jobExecution -> updateSnapshotStatus(jobExecution, params));
    }
  }

  @Override
  public Future<JobExecution> setJobProfileToJobExecution(String jobExecutionId, JobProfileInfo jobProfile, OkapiConnectionParams params) {
    return loadJobProfileById(jobProfile.getId(), params)
      .map(profile-> jobProfile.withName(profile.getName()))
      .compose(v-> jobExecutionDao.updateBlocking(jobExecutionId, jobExecution -> {
        if (jobExecution.getJobProfileSnapshotWrapper() != null) {
          throw new BadRequestException(String.format("JobExecution already associated to JobProfile with id '%s'", jobProfile.getId()));
        }
        return createJobProfileSnapshotWrapper(jobProfile, params)
          .map(profileSnapshotWrapper -> jobExecution
            .withJobProfileInfo(jobProfile)
            .withJobProfileSnapshotWrapper(profileSnapshotWrapper));
      }, params.getTenantId())
        .recover(throwable -> {
          StatusDto statusDto = new StatusDto().withStatus(ERROR).withErrorStatus(PROFILE_SNAPSHOT_CREATING_ERROR);
          return updateJobExecutionStatus(jobExecutionId, statusDto, params)
            .compose(ar -> Future.failedFuture(throwable));
        }));
  }

  @Override
  public Future<JobProfileInfoCollection> getRelatedJobProfiles(int offset, int limit, String tenantId) {
    return jobExecutionDao.getRelatedJobProfiles(offset, limit, tenantId);
  }

  private Future<ProfileSnapshotWrapper> createJobProfileSnapshotWrapper(JobProfileInfo jobProfile, OkapiConnectionParams params) {
    Promise<ProfileSnapshotWrapper> promise = Promise.promise();
    DataImportProfilesClient client = new DataImportProfilesClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());

    client.postDataImportProfilesJobProfileSnapshotsById(jobProfile.getId(), response -> {
      if (response.result().statusCode() == HTTP_CREATED.toInt()) {
          promise.handle(Try.itGet(() -> response.result().bodyAsJsonObject().mapTo(ProfileSnapshotWrapper.class)));
      } else {
        String message = String.format("Error creating ProfileSnapshotWrapper by JobProfile id '%s', response code %s", jobProfile.getId(), response.result().statusCode());
        LOGGER.error(message);
        promise.fail(message);
      }
    });
    return promise.future();
  }

  private Future<JobProfile> loadJobProfileById(String jobProfileId, OkapiConnectionParams params) {
    Promise<JobProfile> promise = Promise.promise();
    DataImportProfilesClient client = new DataImportProfilesClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    client.getDataImportProfilesJobProfilesById(jobProfileId, false, null, response -> {
      if (response.result().statusCode() == HTTP_OK.toInt()) {
        promise.handle(Try.itGet(() -> response.result().bodyAsJsonObject().mapTo(JobProfile.class)));
      } else {
        String message = String.format("Error loading JobProfile by JobProfile id '%s', response code %s", jobProfileId, response.result().statusCode());
        LOGGER.error(message);
        promise.fail(message);
      }
    });

    return promise.future();
  }

  @Override
  public Future<Boolean> completeJobExecutionWithError(String jobExecutionId, OkapiConnectionParams params) {
    return jobExecutionDao.getJobExecutionById(jobExecutionId, params.getTenantId())
      .map(optionalJobExecution -> optionalJobExecution
        .orElseThrow(() -> new NotFoundException(format("JobExecution with id '%s' was not found", jobExecutionId))))
      .map(this::verifyJobExecution)
      .map(this::modifyJobExecutionToCompleteWithCancelledStatus)
      .compose(jobExec -> updateJobExecutionWithSnapshotStatus(jobExec, params))
      .compose(jobExec -> deleteRecordsFromSRSIfNecessary(jobExec, params))
      .map(true)
      .recover(
        throwable -> throwable instanceof JobDuplicateUpdateException ?
          Future.succeededFuture(true) :
          Future.failedFuture(throwable)
      );
  }

  @Override
  public Future<DeleteJobExecutionsResp> softDeleteJobExecutionsByIds(List<String> ids, String tenantId) {
    return jobExecutionDao.softDeleteJobExecutionsByIds(ids, tenantId);
  }

  @Override
  public Future<JobExecutionUserInfoCollection> getRelatedUsersInfo(int offset, int limit, String tenantId) {
    return jobExecutionDao.getRelatedUsersInfo(offset, limit, tenantId);
  }

  /**
   * Creates and returns list of JobExecution entities depending on received files.
   * In a case if only one file passed, method returns list with one JobExecution entity
   * signed by SINGLE_PARENT status.
   * In a case if N files passed (N > 1), method returns list with JobExecution entities
   * with one JobExecution entity signed by PARENT_MULTIPLE and N JobExecution entities signed by CHILD status.
   *
   * @param parentJobExecutionId id of the parent JobExecution entity
   * @param files                Representations of the Files user uploads
   * @param userInfo             The user creating JobExecution
   * @param dto                  {@link InitJobExecutionsRqDto}
   * @return list of JobExecution entities
   */
  private List<JobExecution> prepareJobExecutionList(String parentJobExecutionId, List<File> files, UserInfo userInfo, InitJobExecutionsRqDto dto) {
    String userId = dto.getUserId();
    if (dto.getSourceType().equals(InitJobExecutionsRqDto.SourceType.ONLINE)) {
      JobProfileInfo jobProfileInfo = dto.getJobProfileInfo();
      if (jobProfileInfo != null && jobProfileInfo.getId().equals(DEFAULT_JOB_PROFILE_ID)) {
        jobProfileInfo.withName(DEFAULT_JOB_PROFILE);
      }
      return Collections.singletonList(buildNewJobExecution(true, true, parentJobExecutionId, NO_FILE_NAME, userId)
        .withJobProfileInfo(jobProfileInfo)
        .withRunBy(buildRunByFromUserInfo(userInfo)));
    }
    List<JobExecution> result = new ArrayList<>();
    if (files.size() > 1) {
      for (File file : files) {
        result.add(buildNewJobExecution(false, false, parentJobExecutionId, file.getName(), userId));
      }
      result.add(buildNewJobExecution(true, false, parentJobExecutionId, null, userId));
    } else {
      File file = files.get(0);
      result.add(buildNewJobExecution(true, true, parentJobExecutionId, file.getName(), userId));
    }
    result.forEach(job -> job.setRunBy(buildRunByFromUserInfo(userInfo)));
    return result;
  }

  private RunBy buildRunByFromUserInfo(UserInfo info) {
    RunBy result = new RunBy();
    if (info != null) {
      result.setFirstName(info.getFirstName());
      result.setLastName(info.getLastName());
    }
    return result;
  }

  /**
   * Finds user by user id and returns UserInfo
   *
   * @param userId user id
   * @param params Okapi connection params
   * @return Future with found UserInfo
   */
  private Future<UserInfo> lookupUser(String userId, OkapiConnectionParams params) {
    Promise<UserInfo> promise = Promise.promise();
    RestUtil.doRequest(params, GET_USER_URL + userId, HttpMethod.GET, null)
      .onComplete(getUserResult -> {
        if (RestUtil.validateAsyncResult(getUserResult, promise)) {
          JsonObject response = getUserResult.result().getJson();
          if (!response.containsKey("totalRecords") || !response.containsKey("users")) {
            promise.fail("Error, missing field(s) 'totalRecords' and/or 'users' in user response object");
          } else {
            int recordCount = response.getInteger("totalRecords");
            if (recordCount > 1) {
              String errorMessage = "There are more then one user by requested user id : " + userId;
              LOGGER.error(errorMessage);
              promise.fail(errorMessage);
            } else if (recordCount == 0) {
              String errorMessage = "No user found by user id :" + userId;
              LOGGER.error(errorMessage);
              promise.fail(errorMessage);
            } else {
              JsonObject jsonUser = response.getJsonArray("users").getJsonObject(0);
              JsonObject userPersonalInfo = jsonUser.getJsonObject("personal");
              String userName = jsonUser.getString("username");
              UserInfo userInfo = new UserInfo()
                .withFirstName(Objects.isNull(userPersonalInfo)
                  ? userName  : userPersonalInfo.getString("firstName"))
                .withLastName(Objects.isNull(userPersonalInfo)
                  ? DEFAULT_LASTNAME : userPersonalInfo.getString("lastName"))
                .withUserName(userName);
              promise.complete(userInfo);
            }
          }
        }
      });
    return promise.future();
  }

  /**
   * Create new JobExecution object and fill fields
   */
  private JobExecution buildNewJobExecution(boolean isParent, boolean isSingle, String parentJobExecutionId, String fileName, String userId) {
    JobExecution job = new JobExecution()
      .withId(isParent ? parentJobExecutionId : UUID.randomUUID().toString())
      .withParentJobId(parentJobExecutionId)
      .withSourcePath(fileName)
      .withFileName(FilenameUtils.getName(fileName))
      .withProgress(new Progress()
        .withCurrent(0)
        .withTotal(100))
      .withUserId(userId)
      .withStartedDate(new Date());
    if (!isParent) {
      job.withSubordinationType(JobExecution.SubordinationType.CHILD)
        .withStatus(JobExecution.Status.NEW)
        .withUiStatus(JobExecution.UiStatus.valueOf(Status.valueOf(JobExecution.Status.NEW.value()).getUiStatus()));
    } else {
      job.withSubordinationType(isSingle ? JobExecution.SubordinationType.PARENT_SINGLE : JobExecution.SubordinationType.PARENT_MULTIPLE)
        .withStatus(isSingle ? JobExecution.Status.NEW : JobExecution.Status.PARENT)
        .withUiStatus(isSingle ?
          JobExecution.UiStatus.valueOf(Status.valueOf(JobExecution.Status.NEW.value()).getUiStatus())
          : JobExecution.UiStatus.valueOf(Status.valueOf(JobExecution.Status.PARENT.value()).getUiStatus()));
    }
    return job;
  }

  /**
   * Creates and returns list of Snapshot entities depending on received JobExecution entities.
   * For each JobExecution signed by SINGLE_PARENT or CHILD status
   * method creates Snapshot entity.
   *
   * @param jobExecutions list of JobExecution entities
   * @return returns list of Snapshot entities
   */
  private List<Snapshot> prepareSnapshotList(List<JobExecution> jobExecutions) {
    List<Snapshot> snapshotList = new ArrayList<>();
    for (JobExecution jobExecution : jobExecutions) {
      if (!JobExecution.SubordinationType.PARENT_MULTIPLE.equals(jobExecution.getSubordinationType())) {
        snapshotList.add(new Snapshot().withJobExecutionId(jobExecution.getId())
          .withStatus(Snapshot.Status.NEW));
      }
    }
    return snapshotList;
  }

  /**
   * Performs save for received JobExecution entities using {@link JobExecutionDao}
   *
   * @param jobExecutions list on JobExecution entities
   * @return future
   */
  private Future<List<String>> saveJobExecutions(List<JobExecution> jobExecutions, String tenantId) {
    List<Future<String>> savedJobExecutionFutures = new ArrayList<>();
    for (JobExecution jobExecution : jobExecutions) {
      Future<String> savedJobExecutionFuture = jobExecutionDao.save(jobExecution, tenantId);
      savedJobExecutionFutures.add(savedJobExecutionFuture);
    }
    return GenericCompositeFuture.all(savedJobExecutionFutures).map(genericCompositeFuture -> genericCompositeFuture.result().list());
  }

  /**
   * Performs save for received Snapshot entities.
   * For each Snapshot posts the request to mod-source-record-manager.
   *
   * @param snapshots list of Snapshot entities
   * @param params    object-wrapper with params necessary to connect to OKAPI
   * @return future
   */
  private Future<List<String>> saveSnapshots(List<Snapshot> snapshots, OkapiConnectionParams params) {
    List<Future<String>> postedSnapshotFutures = new ArrayList<>();
    for (Snapshot snapshot : snapshots) {
      Future<String> postedSnapshotFuture = postSnapshot(snapshot, params);
      postedSnapshotFutures.add(postedSnapshotFuture);
    }
    return GenericCompositeFuture.all(postedSnapshotFutures).map(genericCompositeFuture -> genericCompositeFuture.result().list());
  }

  /**
   * Performs post request with given Snapshot entity.
   *
   * @param snapshot Snapshot entity
   * @param params   object-wrapper with params necessary to connect to OKAPI
   * @return future
   */
  private Future<String> postSnapshot(Snapshot snapshot, OkapiConnectionParams params) {
    Promise<String> promise = Promise.promise();

    SourceStorageSnapshotsClient client = new SourceStorageSnapshotsClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.postSourceStorageSnapshots(null, snapshot, response -> {
        if (response.result().statusCode() != HttpStatus.HTTP_CREATED.toInt()) {
          LOGGER.error("Error during post for new Snapshot. Status message: {}", response.result().statusMessage());
          promise.fail(new HttpException(response.result().statusCode(), "Error during post for new Snapshot."));
        } else {
          promise.complete(response.result().bodyAsString());
        }
      });
    } catch (Exception e) {
      LOGGER.error("Error during post for new Snapshot", e);
      promise.fail(e);
    }
    return promise.future();
  }

  private Future<JobExecution> updateSnapshotStatus(JobExecution jobExecution, OkapiConnectionParams params) {
    Promise<JobExecution> promise = Promise.promise();
    Snapshot snapshot = new Snapshot()
      .withJobExecutionId(jobExecution.getId())
      .withStatus(Snapshot.Status.fromValue(jobExecution.getStatus().name()));

    SourceStorageSnapshotsClient client = new SourceStorageSnapshotsClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.putSourceStorageSnapshotsByJobExecutionId(jobExecution.getId(), null, snapshot, response -> {
        if (response.result().statusCode() == HttpStatus.HTTP_OK.toInt()) {
          promise.complete(jobExecution);
        } else {
          jobExecutionDao.updateBlocking(jobExecution.getId(), jobExec -> {
            Promise<JobExecution> jobExecutionPromise = Promise.promise();
            jobExec.setErrorStatus(JobExecution.ErrorStatus.SNAPSHOT_UPDATE_ERROR);
            jobExec.setStatus(JobExecution.Status.ERROR);
            jobExec.setUiStatus(JobExecution.UiStatus.ERROR);
            jobExec.setCompletedDate(new Date());
            jobExecutionPromise.complete(jobExec);
            return jobExecutionPromise.future();
          }, params.getTenantId()).onComplete(jobExecutionUpdate -> {
            String message = "Couldn't update snapshot status for jobExecution with id " + jobExecution.getId();
            LOGGER.error(message);
            promise.fail(message);
          });
        }
      });
    } catch (Exception e) {
      LOGGER.error("Error during update for Snapshot with id {}", jobExecution.getId(), e);
      promise.fail(e);
    }
    return promise.future();
  }

  private JobExecution verifyJobExecution(JobExecution jobExecution) {
    if (jobExecution.getStatus() == JobExecution.Status.ERROR || jobExecution.getStatus() == COMMITTED
    || jobExecution.getStatus() == JobExecution.Status.CANCELLED) {
      String msg = String.format("JobExecution with status '%s' cannot be forcibly completed", jobExecution.getStatus());
      LOGGER.info(msg);
      throw new JobDuplicateUpdateException(msg);
    }
    return jobExecution;
  }

  private JobExecution modifyJobExecutionToCompleteWithCancelledStatus(JobExecution jobExecution) {
    return jobExecution
      .withStatus(JobExecution.Status.CANCELLED)
      .withUiStatus(JobExecution.UiStatus.CANCELLED)
      .withCompletedDate(new Date());
  }

  private Future<Boolean> deleteRecordsFromSRSIfNecessary(JobExecution jobExecution, OkapiConnectionParams params) {
    if (!jobExecution.getJobProfileInfo().getId().equals(DEFAULT_JOB_PROFILE_ID)) {
      LOGGER.info("Records removing was skipped because JobExecution is not processed by default job profile");
      return Future.succeededFuture(false);
    }
    return deleteRecordsFromSRS(jobExecution.getId(), params);
  }

  private Future<Boolean> deleteRecordsFromSRS(String jobExecutionId, OkapiConnectionParams params) {
    Promise<Boolean> promise = Promise.promise();
    SourceStorageSnapshotsClient client = new SourceStorageSnapshotsClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.deleteSourceStorageSnapshotsByJobExecutionId(jobExecutionId, null, response -> {
        if (response.result().statusCode() == HttpStatus.HTTP_NO_CONTENT.toInt()) {
          promise.complete(true);
        } else {
          String message = format("Records from SRS were not deleted for JobExecution %s", jobExecutionId);
          LOGGER.error(message);
          promise.fail(new HttpException(response.result().statusCode(), message));
        }
      });
    } catch (Exception e) {
      LOGGER.error("Error deleting records from SRS for Job Execution {}", jobExecutionId, e);
      promise.fail(e);
    }
    return promise.future();
  }

  /**
   * Updates jobExecution object, if Error exists.
   *
   * @param status       - DTO which contains new status
   * @param jobExecution - specific JobExecution
   */
  private void updateJobExecutionIfErrorExist(StatusDto status, JobExecution jobExecution) {
    if (status.getStatus() == ERROR) {
      jobExecution.setErrorStatus(JobExecution.ErrorStatus.fromValue(status.getErrorStatus().name()));
      jobExecution.setCompletedDate(new Date());
      if (jobExecution.getErrorStatus().equals(JobExecution.ErrorStatus.FILE_PROCESSING_ERROR)) {
        jobExecution.setProgress(jobExecution.getProgress().withTotal(0));
      }
    }
    else if (status.getStatus() == CANCELLED) {
      jobExecution.setCompletedDate(new Date());
    }
  }
}
