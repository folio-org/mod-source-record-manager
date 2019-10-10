package org.folio.services;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.handler.impl.HttpStatusException;
import org.folio.HttpStatus;
import org.folio.dao.JobExecutionDao;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.rest.client.SourceStorageClient;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.jaxrs.model.JobExecutionCollectionDto;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.LogCollectionDto;
import org.folio.rest.jaxrs.model.Progress;
import org.folio.rest.jaxrs.model.RunBy;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.UserInfo;
import org.folio.services.converters.JobExecutionToDtoConverter;
import org.folio.services.converters.JobExecutionToLogDtoConverter;
import org.folio.services.converters.Status;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static java.lang.String.format;

/**
 * Implementation of the JobExecutionService, calls JobExecutionDao to access JobExecution metadata.
 *
 * @see JobExecutionService
 * @see JobExecutionDao
 * @see JobExecution
 */
@Service
public class JobExecutionServiceImpl implements JobExecutionService {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutionServiceImpl.class);
  private static final String GET_USER_URL = "/users?query=id==";

  @Autowired
  private JobExecutionDao jobExecutionDao;
  @Autowired
  private JobExecutionSourceChunkDao jobExecutionSourceChunkDao;
  @Autowired
  private JobExecutionToDtoConverter jobExecutionToDtoConverter;
  @Autowired
  private JobExecutionToLogDtoConverter jobExecutionToLogDtoConverter;
  private Random random = new Random();

  @Override
  public Future<JobExecutionCollectionDto> getJobExecutionCollectionDtoByQuery(String query, int offset, int limit, String tenantId) {
    return jobExecutionDao.getJobExecutionsWithoutParentMultiple(query, offset, limit, tenantId)
      .map(jobExecutionCollection -> new JobExecutionCollectionDto()
        .withJobExecutionDtos(jobExecutionToDtoConverter.convert(jobExecutionCollection.getJobExecutions()))
        .withTotalRecords(jobExecutionCollection.getTotalRecords()));
  }

  @Override
  public Future<LogCollectionDto> getLogCollectionDtoByQuery(String query, int offset, int limit, String tenantId) {
    return jobExecutionDao.getLogsWithoutMultipleParent(query, offset, limit, tenantId)
      .map(jobExecutionCollection -> new LogCollectionDto()
        .withLogDtos(jobExecutionToLogDtoConverter.convert(jobExecutionCollection.getJobExecutions()))
        .withTotalRecords(jobExecutionCollection.getTotalRecords()));
  }

  @Override
  public Future<InitJobExecutionsRsDto> initializeJobExecutions(InitJobExecutionsRqDto jobExecutionsRqDto, OkapiConnectionParams params) {
    if (jobExecutionsRqDto.getSourceType().equals(InitJobExecutionsRqDto.SourceType.FILES) && jobExecutionsRqDto.getFiles().isEmpty()) {
      String errorMessage = "Received files must not be empty";
      LOGGER.error(errorMessage);
      return Future.failedFuture(new BadRequestException(errorMessage));
    } else if (jobExecutionsRqDto.getSourceType().equals(InitJobExecutionsRqDto.SourceType.ONLINE) && jobExecutionsRqDto.getJobProfileInfo() == null) {
      String errorMessage = "Received jobProfileInfo must not be empty";
      LOGGER.error(errorMessage);
      return Future.failedFuture(new BadRequestException(errorMessage));
    } else {
      String parentJobExecutionId = UUID.randomUUID().toString();
      return lookupUser(jobExecutionsRqDto.getUserId(), params)
        .compose(userInfo -> {
          List<JobExecution> jobExecutions =
            prepareJobExecutionList(parentJobExecutionId, jobExecutionsRqDto.getFiles(), userInfo, jobExecutionsRqDto);
          List<Snapshot> snapshots = prepareSnapshotList(jobExecutions);
          Future savedJsonExecutionsFuture = saveJobExecutions(jobExecutions, params.getTenantId());
          Future savedSnapshotsFuture = saveSnapshots(snapshots, params);
          return CompositeFuture.all(savedJsonExecutionsFuture, savedSnapshotsFuture)
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
      Future<JobExecution> future = Future.future();
      if (JobExecution.Status.PARENT.equals(jobExecution.getStatus()) ^ JobExecution.Status.PARENT.equals(currentJobExec.getStatus())) {
        String errorMessage = format("JobExecution %s current status is %s and cannot be updated to %s",
          currentJobExec.getId(), currentJobExec.getStatus(), jobExecution.getStatus());
        LOGGER.error(errorMessage);
        future.fail(new BadRequestException(errorMessage));
      } else {
        currentJobExec = jobExecution;
        future.complete(currentJobExec);
      }
      return future;
    }, params.getTenantId());
  }

  @Override
  public Future<Optional<JobExecution>> getJobExecutionById(String id, String tenantId) {
    return jobExecutionDao.getJobExecutionById(id, tenantId);
  }

  @Override
  public Future<JobExecutionCollection> getJobExecutionCollectionByParentId(String parentId, String query, int offset, int limit, String tenantId) {
    return jobExecutionDao.getJobExecutionById(parentId, tenantId)
      .compose(optionalJobExecution -> optionalJobExecution
        .map(jobExec -> {
          if (JobExecution.SubordinationType.PARENT_MULTIPLE.equals(jobExec.getSubordinationType())) {
            return jobExecutionDao.getChildrenJobExecutionsByParentId(jobExec.getId(), query, offset, limit, tenantId);
          } else {
            return Future.succeededFuture(new JobExecutionCollection().withTotalRecords(0));
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
        Future<JobExecution> future = Future.future();
        try {
          if (JobExecution.Status.PARENT.name().equals(jobExecution.getStatus().name())) {
            String message = format("JobExecution %s current status is PARENT and cannot be updated", jobExecutionId);
            LOGGER.error(message);
            future.fail(new BadRequestException(message));
          } else {
            jobExecution.setStatus(JobExecution.Status.fromValue(status.getStatus().name()));
            jobExecution.setUiStatus(JobExecution.UiStatus.fromValue(Status.valueOf(status.getStatus().name()).getUiStatus()));
            if (status.getStatus().equals(StatusDto.Status.ERROR)) {
              jobExecution.withErrorStatus(JobExecution.ErrorStatus.fromValue(status.getErrorStatus().name()))
                .withCompletedDate(new Date());
            }
            future.complete(jobExecution);
          }
        } catch (Exception e) {
          String errorMessage = "Error updating JobExecution with id " + jobExecutionId;
          LOGGER.error(errorMessage, e);
          future.fail(errorMessage);
        }
        return future;
      }, params.getTenantId())
        .compose(jobExecution -> updateSnapshotStatus(jobExecution, params));
    }
  }

  @Override
  public Future<JobExecution> setJobProfileToJobExecution(String jobExecutionId, JobProfileInfo jobProfile, String tenantId) {
    return jobExecutionDao.updateBlocking(jobExecutionId, jobExecution -> {
      Future<JobExecution> future = Future.future();
      try {
        jobExecution.setJobProfileInfo(jobProfile);
        future.complete(jobExecution);
      } catch (Exception e) {
        String errorMessage = "Error setting JobProfile to JobExecution with id " + jobExecutionId;
        LOGGER.error(errorMessage, e);
        future.fail(errorMessage);
      }
      return future;
    }, tenantId);
  }

  @Override
  public Future<Boolean> deleteJobExecutionAndSRSRecords(String jobExecutionId, OkapiConnectionParams params) {
    return jobExecutionDao.getJobExecutionById(jobExecutionId, params.getTenantId())
      .compose(optionalJobExecution -> optionalJobExecution
        .map(jobExec -> deleteRecordsFromSRS(jobExecutionId, params)
          .compose(v -> jobExecutionSourceChunkDao.deleteByJobExecutionId(jobExecutionId, params.getTenantId()))
          .compose(v -> jobExecutionDao.deleteJobExecutionById(jobExecutionId, params.getTenantId())))
        .orElse(Future.failedFuture(new NotFoundException(
          format("JobExecution with id '%s' was not found", jobExecutionId))))
      );
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
      return Collections.singletonList(buildNewJobExecution(true, true, parentJobExecutionId, null, userId)
        .withJobProfileInfo(dto.getJobProfileInfo())
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
    Future<UserInfo> future = Future.future();
    RestUtil.doRequest(params, GET_USER_URL + userId, HttpMethod.GET, null)
      .setHandler(getUserResult -> {
        if (RestUtil.validateAsyncResult(getUserResult, future)) {
          JsonObject response = getUserResult.result().getJson();
          if (!response.containsKey("totalRecords") || !response.containsKey("users")) {
            future.fail("Error, missing field(s) 'totalRecords' and/or 'users' in user response object");
          } else {
            int recordCount = response.getInteger("totalRecords");
            if (recordCount > 1) {
              String errorMessage = "There are more then one user by requested user id : " + userId;
              LOGGER.error(errorMessage);
              future.fail(errorMessage);
            } else if (recordCount == 0) {
              String errorMessage = "No user found by user id :" + userId;
              LOGGER.error(errorMessage);
              future.fail(errorMessage);
            } else {
              JsonObject jsonUser = response.getJsonArray("users").getJsonObject(0);
              JsonObject userPersonalInfo = jsonUser.getJsonObject("personal");
              UserInfo userInfo = new UserInfo()
                .withFirstName(userPersonalInfo.getString("firstName"))
                .withLastName(userPersonalInfo.getString("lastName"))
                .withUserName(jsonUser.getString("username"));
              future.complete(userInfo);
            }
          }
        }
      });
    return future;
  }

  /**
   * Create new JobExecution object and fill fields
   */
  private JobExecution buildNewJobExecution(boolean isParent, boolean isSingle, String parentJobExecutionId, String fileName, String userId) {
    JobExecution job = new JobExecution()
      .withId(isParent ? parentJobExecutionId : UUID.randomUUID().toString())
      // stub hrId
      .withHrId(String.valueOf(random.nextInt(99999)))
      .withParentJobId(parentJobExecutionId)
      .withSourcePath(fileName)
      .withProgress(new Progress()
        .withCurrent(1)
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
    List<Future> savedJobExecutionFutures = new ArrayList<>();
    for (JobExecution jobExecution : jobExecutions) {
      Future<String> savedJobExecutionFuture = jobExecutionDao.save(jobExecution, tenantId);
      savedJobExecutionFutures.add(savedJobExecutionFuture);
    }
    return CompositeFuture.all(savedJobExecutionFutures).map(compositeFuture -> compositeFuture.result().list());
  }

  /**
   * Performs save for received Snapshot entities.
   * For each Snapshot posts the request to mod-source-record-manager.
   *
   * @param snapshots list of Snapshot entities
   * @param params    object-wrapper with params necessary to connect to OKAPI
   * @return future
   */
  private Future saveSnapshots(List<Snapshot> snapshots, OkapiConnectionParams params) {
    List<Future> postedSnapshotFutures = new ArrayList<>();
    for (Snapshot snapshot : snapshots) {
      Future<String> postedSnapshotFuture = postSnapshot(snapshot, params);
      postedSnapshotFutures.add(postedSnapshotFuture);
    }
    return CompositeFuture.all(postedSnapshotFutures).map(compositeFuture -> compositeFuture.result().list());
  }

  /**
   * Performs post request with given Snapshot entity.
   *
   * @param snapshot Snapshot entity
   * @param params   object-wrapper with params necessary to connect to OKAPI
   * @return future
   */
  private Future<String> postSnapshot(Snapshot snapshot, OkapiConnectionParams params) {
    Future<String> future = Future.future();

    SourceStorageClient client = new SourceStorageClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.postSourceStorageSnapshots(null, snapshot, response -> {
        if (response.statusCode() != HttpStatus.HTTP_CREATED.toInt()) {
          LOGGER.error("Error during post for new Snapshot.", response.statusMessage());
          future.fail(new HttpStatusException(response.statusCode(), "Error during post for new Snapshot."));
        } else {
          response.bodyHandler(buffer -> future.complete(buffer.toString()));
        }
      });
    } catch (Exception e) {
      LOGGER.error("Error during post for new Snapshot", e);
      future.fail(e);
    }
    return future;
  }

  private Future<JobExecution> updateSnapshotStatus(JobExecution jobExecution, OkapiConnectionParams params) {
    Future<JobExecution> future = Future.future();
    Snapshot snapshot = new Snapshot()
      .withJobExecutionId(jobExecution.getId())
      .withStatus(Snapshot.Status.fromValue(jobExecution.getStatus().name()));

    SourceStorageClient client = new SourceStorageClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.putSourceStorageSnapshotsByJobExecutionId(jobExecution.getId(), null, snapshot, response -> {
        if (response.statusCode() == HttpStatus.HTTP_OK.toInt()) {
          future.complete(jobExecution);
        } else {
          jobExecutionDao.updateBlocking(jobExecution.getId(), jobExec -> {
            Future<JobExecution> jobExecutionFuture = Future.future();
            jobExec.setErrorStatus(JobExecution.ErrorStatus.SNAPSHOT_UPDATE_ERROR);
            jobExec.setStatus(JobExecution.Status.ERROR);
            jobExec.setUiStatus(JobExecution.UiStatus.ERROR);
            jobExecutionFuture.complete(jobExec);
            return jobExecutionFuture;
          }, params.getTenantId()).setHandler(jobExecutionUpdate -> {
            String message = "Couldn't update snapshot status for jobExecution with id " + jobExecution.getId();
            LOGGER.error(message);
            future.fail(message);
          });
        }
      });
    } catch (Exception e) {
      LOGGER.error("Error during update for Snapshot with id {}", e, jobExecution.getId());
      future.fail(e);
    }
    return future;
  }

  private Future<Boolean> deleteRecordsFromSRS(String jobExecutionId, OkapiConnectionParams params) {
    Future<Boolean> future = Future.future();
    SourceStorageClient client = new SourceStorageClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.deleteSourceStorageSnapshotsRecordsByJobExecutionId(jobExecutionId, response -> {
        if (response.statusCode() == HttpStatus.HTTP_NO_CONTENT.toInt()) {
          future.complete(true);
        } else {
          String message = format("Records from SRS were not deleted for JobExecution %s", jobExecutionId);
          LOGGER.error(message);
          future.fail(new HttpStatusException(response.statusCode(), message));
        }
      });
    } catch (Exception e) {
      LOGGER.error("Error deleting records from SRS for Job Execution {}", e, jobExecutionId);
      future.fail(e);
    }
    return future;
  }
}
