package org.folio.services;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.http.HttpStatus;
import org.folio.dao.JobExecutionDao;
import org.folio.dao.JobExecutionDaoImpl;
import org.folio.dataImport.util.OkapiConnectionParams;
import org.folio.dataImport.util.RestUtil;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.jaxrs.model.JobExecutionCollectionDto;
import org.folio.rest.jaxrs.model.JobProfile;
import org.folio.rest.jaxrs.model.LogCollectionDto;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.converters.JobExecutionToDtoConverter;
import org.folio.services.converters.JobExecutionToLogDtoConverter;
import org.folio.services.converters.Status;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * Implementation of the JobExecutionService, calls JobExecutionDao to access JobExecution metadata.
 *
 * @see JobExecutionService
 * @see JobExecutionDao
 * @see JobExecution
 */
public class JobExecutionServiceImpl implements JobExecutionService {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutionServiceImpl.class);
  public static final String SNAPSHOT_SERVICE_URL = "/source-storage/snapshot";
  private JobExecutionDao jobExecutionDao;
  private JobExecutionToDtoConverter jobExecutionToDtoConverter;
  private JobExecutionToLogDtoConverter jobExecutionToLogDtoConverter;

  public JobExecutionServiceImpl(Vertx vertx, String tenantId) {
    this.jobExecutionDao = new JobExecutionDaoImpl(vertx, tenantId);
    this.jobExecutionToDtoConverter = new JobExecutionToDtoConverter();
    this.jobExecutionToLogDtoConverter = new JobExecutionToLogDtoConverter();
  }

  @Override
  public Future<JobExecutionCollectionDto> getJobExecutionCollectionDtoByQuery(String query, int offset, int limit) {
    return jobExecutionDao.getJobExecutionsWithoutParentMultiple(query, offset, limit)
      .map(jobExecutionCollection -> new JobExecutionCollectionDto()
        .withJobExecutionDtos(jobExecutionToDtoConverter.convert(jobExecutionCollection.getJobExecutions()))
        .withTotalRecords(jobExecutionCollection.getTotalRecords()));
  }

  @Override
  public Future<LogCollectionDto> getLogCollectionDtoByQuery(String query, int offset, int limit) {
    return jobExecutionDao.getLogsWithoutMultipleParent(query, offset, limit)
      .map(jobExecutionCollection -> new LogCollectionDto()
        .withLogDtos(jobExecutionToLogDtoConverter.convert(jobExecutionCollection.getJobExecutions()))
        .withTotalRecords(jobExecutionCollection.getTotalRecords()));
  }

  @Override
  public Future<InitJobExecutionsRsDto> initializeJobExecutions(InitJobExecutionsRqDto jobExecutionsRqDto, OkapiConnectionParams params) {
    if (jobExecutionsRqDto.getFiles().isEmpty()) {
      String errorMessage = "Received files must not be empty";
      LOGGER.error(errorMessage);
      return Future.failedFuture(new BadRequestException(errorMessage));
    } else {
      String parentJobExecutionId = UUID.randomUUID().toString();

      List<JobExecution> jobExecutions =
        prepareJobExecutionList(parentJobExecutionId, jobExecutionsRqDto.getFiles(), jobExecutionsRqDto.getUserId());
      List<Snapshot> snapshots = prepareSnapshotList(jobExecutions);

      Future savedJsonExecutionsFuture = saveJobExecutions(jobExecutions);
      Future savedSnapshotsFuture = saveSnapshots(snapshots, params);

      return CompositeFuture.all(savedJsonExecutionsFuture, savedSnapshotsFuture)
        .map(new InitJobExecutionsRsDto()
          .withParentJobExecutionId(parentJobExecutionId)
          .withJobExecutions(jobExecutions));
    }
  }

  @Override
  public Future<JobExecution> updateJobExecution(JobExecution jobExecution, OkapiConnectionParams params) {
    return jobExecutionDao.updateJobExecution(jobExecution)
      .compose(jobExec -> updateSnapshotStatus(jobExecution, params));
  }

  @Override
  public Future<Optional<JobExecution>> getJobExecutionById(String id) {
    return jobExecutionDao.getJobExecutionById(id);
  }

  @Override
  public Future<JobExecutionCollection> getJobExecutionCollectionByParentId(String parentId) {
    return jobExecutionDao.getJobExecutionById(parentId)
      .compose(optionalJobExecution -> optionalJobExecution
        .map(jobExec -> {
          if (JobExecution.SubordinationType.PARENT_MULTIPLE.equals(jobExec.getSubordinationType())) {
            return jobExecutionDao.getChildrenJobExecutionsByParentId(jobExec.getId());
          } else {
            return Future.succeededFuture(new JobExecutionCollection().withTotalRecords(0));
          }
        })
        .orElse(Future.failedFuture(new NotFoundException(
          String.format("JobExecution with id '%s' was not found", parentId))))
      );
  }

  @Override
  public Future<JobExecution> updateJobExecutionStatus(String jobExecutionId, StatusDto status, OkapiConnectionParams params) {
    return jobExecutionDao.updateBlocking(jobExecutionId, jobExecution -> {
      Future<JobExecution> future = Future.future();
      try {
        jobExecution.setStatus(JobExecution.Status.fromValue(status.getStatus().name()));
        jobExecution.setUiStatus(JobExecution.UiStatus.fromValue(Status.valueOf(status.getStatus().name()).getUiStatus()));
        if (status.getStatus().equals(StatusDto.Status.ERROR)) {
          jobExecution.setErrorStatus(JobExecution.ErrorStatus.fromValue(status.getErrorStatus().name()));
        }
        future.complete(jobExecution);
      } catch (Exception e) {
        String errorMessage = "Error updating JobExecution with id " + jobExecutionId;
        LOGGER.error(errorMessage, e);
        future.fail(errorMessage);
      }
      return future;
    }).compose(jobExecution -> updateSnapshotStatus(jobExecution, params));
  }

  @Override
  public Future<JobExecution> setJobProfileToJobExecution(String jobExecutionId, JobProfile jobProfile) {
    return jobExecutionDao.updateBlocking(jobExecutionId, jobExecution -> {
      Future<JobExecution> future = Future.future();
      try {
        jobExecution.setJobProfile(jobProfile);
        future.complete(jobExecution);
      } catch (Exception e) {
        String errorMessage = "Error setting JobProfile to JobExecution with id " + jobExecutionId;
        LOGGER.error(errorMessage, e);
        future.fail(errorMessage);
      }
      return future;
    });
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
   * @param userId               id of the user creating JobExecution
   * @return list of JobExecution entities
   */
  private List<JobExecution> prepareJobExecutionList(String parentJobExecutionId, List<File> files, String userId) {
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
    return result;
  }

  /**
   * Create new JobExecution object and fill fields
   */
  private JobExecution buildNewJobExecution(boolean isParent, boolean isSingle, String parentJobExecutionId, String fileName, String userId) {
    JobExecution job = new JobExecution()
      .withId(isParent ? parentJobExecutionId : UUID.randomUUID().toString())
      .withParentJobId(parentJobExecutionId)
      .withSourcePath(fileName)
      .withUserId(userId);
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
  private Future<List<String>> saveJobExecutions(List<JobExecution> jobExecutions) {
    List<Future> savedJobExecutionFutures = new ArrayList<>();
    for (JobExecution jobExecution : jobExecutions) {
      Future<String> savedJobExecutionFuture = jobExecutionDao.save(jobExecution);
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
    RestUtil.doRequest(params, SNAPSHOT_SERVICE_URL, HttpMethod.POST, snapshot)
      .setHandler(responseResult -> {
        try {
          if (responseResult.failed() || responseResult.result() == null || responseResult.result().getCode() != HttpStatus.SC_CREATED ) {
            LOGGER.error("Error during post for new Snapshot.", responseResult.cause());
            future.fail(responseResult.cause());
          } else {
            String responseBody = responseResult.result().getBody();
            future.complete(responseBody);
          }
        } catch (Exception e) {
          LOGGER.error("Error during post for new Snapshot", e, e.getMessage());
          future.fail(e);
        }
      });
    return future;
  }

  private Future<JobExecution> updateSnapshotStatus(JobExecution jobExecution, OkapiConnectionParams params) {
    Future<JobExecution> future = Future.future();
    String url = SNAPSHOT_SERVICE_URL + "/" + jobExecution.getId();
    Snapshot snapshot = new Snapshot()
      .withJobExecutionId(jobExecution.getId())
      .withStatus(Snapshot.Status.fromValue(jobExecution.getStatus().name()));
    RestUtil.doRequest(params, url, HttpMethod.PUT, snapshot)
      .setHandler(updateResult -> {
        if (RestUtil.validateAsyncResult(updateResult, future)) {
          future.complete(jobExecution);
        } else {
          jobExecutionDao.updateBlocking(jobExecution.getId(), jobExec -> {
            Future<JobExecution> jobExecutionFuture = Future.future();
            jobExec.setErrorStatus(JobExecution.ErrorStatus.SNAPSHOT_UPDATE_ERROR);
            jobExec.setStatus(JobExecution.Status.ERROR);
            jobExec.setUiStatus(JobExecution.UiStatus.ERROR);
            jobExecutionFuture.complete(jobExec);
            return jobExecutionFuture;
          }).setHandler(jobExecutionUpdate -> {
            String message = "Couldn't update snapshot status for jobExecution with id " + jobExecution.getId();
            LOGGER.error(message);
            future.fail(message);
          });
        }
      });
    return future;
  }

}
