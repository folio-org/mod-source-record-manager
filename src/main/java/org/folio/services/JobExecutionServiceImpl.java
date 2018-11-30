package org.folio.services;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dao.JobExecutionDao;
import org.folio.dao.JobExecutionDaoImpl;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollectionDto;
import org.folio.rest.jaxrs.model.LogCollectionDto;
import org.folio.services.converters.JobExecutionToDtoConverter;
import org.folio.services.converters.JobExecutionToLogDtoConverter;
import org.folio.util.OkapiConnectionParams;
import org.folio.util.RestUtil;

import javax.ws.rs.BadRequestException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.folio.util.RestUtil.CREATED_STATUS_CODE;

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
    return jobExecutionDao.getJobExecutions(query, offset, limit)
      .map(jobExecutionCollection -> new JobExecutionCollectionDto()
        .withJobExecutionDtos(jobExecutionToDtoConverter.convert(jobExecutionCollection.getJobExecutions()))
        .withTotalRecords(jobExecutionCollection.getTotalRecords()));
  }

  @Override
  public Future<LogCollectionDto> getLogCollectionDtoByQuery(String query, int offset, int limit) {
    return jobExecutionDao.getJobExecutions(query, offset, limit)
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

      List<JobExecution> jobExecutions = prepareJobExecutionList(parentJobExecutionId, jobExecutionsRqDto.getFiles());
      List<JsonObject> snapshots = prepareSnapshotList(jobExecutions);

      Future savedJsonExecutionsFuture = saveJobExecutions(jobExecutions);
      Future savedSnapshotsFuture = saveSnapshots(snapshots, params);

      return CompositeFuture.all(savedJsonExecutionsFuture, savedSnapshotsFuture)
        .map(new InitJobExecutionsRsDto()
          .withParentJobExecutionId(parentJobExecutionId)
          .withJobExecutions(jobExecutions));
    }
  }

  @Override
  public Future<JobExecution> updateJobExecution(JobExecution jobExecution) {
    return jobExecutionDao.getJobExecutionById(jobExecution.getId())
      .compose(optionalJobExecution -> optionalJobExecution
        .map(jobExec -> {
          if (JobExecution.SubordinationType.PARENT_MULTIPLE.equals(jobExec.getSubordinationType())) {
            return jobExecutionDao.getJobExecutionsByParentId(jobExec.getId())
              .compose(children -> updateChildJobExecutions(children, jobExecution))
              .compose(succeeded -> succeeded ?
                Future.succeededFuture(jobExecution) :
                Future.failedFuture(new InternalServerErrorException(
                  String.format("Could not update child jobExecutions with parent id '%s'", jobExecution.getId()))));
          } else {
            return jobExecutionDao.updateJobExecution(jobExecution);
          }
        })
        .orElse(Future.failedFuture(new NotFoundException(
          String.format("JobExecution with id '%s' was not found", jobExecution.getId()))))
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
   * @param files Representations of the Files user uploads
   * @return list of JobExecution entities
   */
  private List<JobExecution> prepareJobExecutionList(String parentJobExecutionId, List<File> files) {
    List<JobExecution> result = new ArrayList<>();
    if (files.size() > 1) {
      for (File file : files) {
        JobExecution child = new JobExecution()
          .withId(UUID.randomUUID().toString())
          .withParentJobId(parentJobExecutionId)
          .withSubordinationType(JobExecution.SubordinationType.CHILD)
          .withStatus(JobExecution.Status.NEW)
          .withSourcePath(file.getName());
        result.add(child);
      }
      JobExecution parentMultiple = new JobExecution()
        .withId(parentJobExecutionId)
        .withParentJobId(parentJobExecutionId)
        .withSubordinationType(JobExecution.SubordinationType.PARENT_MULTIPLE)
        .withStatus(JobExecution.Status.NEW);
      result.add(parentMultiple);
    } else {
      File file = files.get(0);
      JobExecution parentSingle = new JobExecution()
        .withId(parentJobExecutionId)
        .withParentJobId(parentJobExecutionId)
        .withSubordinationType(JobExecution.SubordinationType.PARENT_SINGLE)
        .withStatus(JobExecution.Status.NEW)
        .withSourcePath(file.getName());
      result.add(parentSingle);
    }
    return result;
  }

  /**
   * Creates and returns list of Snapshot entities (represented as JsonObject) depending on received JobExecution entities.
   * For each JobExecution signed by SINGLE_PARENT or CHILD status
   * method creates Snapshot entity.
   *
   * @param jobExecutions list of JobExecution entities
   * @return returns list of Snapshot entities (represented as JsonObject)
   */
  private List<JsonObject> prepareSnapshotList(List<JobExecution> jobExecutions) {
    List<JsonObject> jsonSnapshots = new ArrayList<>();
    for (JobExecution jobExecution : jobExecutions) {
      if (!JobExecution.SubordinationType.PARENT_MULTIPLE.equals(jobExecution.getSubordinationType())) {
        JsonObject jsonSnapshot = new JsonObject();
        jsonSnapshot.put("jobExecutionId", jobExecution.getId());
        jsonSnapshot.put("status", JobExecution.Status.NEW.name());
        jsonSnapshots.add(jsonSnapshot);
      }
    }
    return jsonSnapshots;
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
   * @param params object-wrapper with params necessary to connect to OKAPI
   * @return future
   */
  private Future saveSnapshots(List<JsonObject> snapshots, OkapiConnectionParams params) {
    List<Future> postedSnapshotFutures = new ArrayList<>();
    for (JsonObject snapshot : snapshots) {
      Future<String> postedSnapshotFuture = postSnapshot(snapshot, params);
      postedSnapshotFutures.add(postedSnapshotFuture);
    }
    return CompositeFuture.all(postedSnapshotFutures).map(compositeFuture -> compositeFuture.result().list());
  }

  /**
   * Performs post request with given Snapshot entity.
   *
   * @param snapshot Snapshot entity (represented as JsonObject)
   * @param params object-wrapper with params necessary to connect to OKAPI
   * @return future
   */
  private Future<String> postSnapshot(JsonObject snapshot, OkapiConnectionParams params) {
    Future<String> future = Future.future();
    RestUtil.doRequest(params, SNAPSHOT_SERVICE_URL, HttpMethod.POST, snapshot.encode())
      .setHandler(responseResult -> {
        try {
          int responseCode = responseResult.result().getCode();
          if (responseResult.failed() || responseCode != CREATED_STATUS_CODE) {
            LOGGER.error("Error during post for new Snapshot. Response code: " + responseCode, responseResult.cause());
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

  /**
   * Sets fields that can be updated and calls jobExecutionDao to update child JobExecutions
   *
   * @param children list of child JobExecutions
   * @param parent parent JobExecution to the list of children
   * @return future with true if succeeded
   */
  private Future<Boolean> updateChildJobExecutions(List<JobExecution> children, JobExecution parent) {
    List<Future> futures = new ArrayList<>();
    for (JobExecution child : children) {
      child.setJobProfileName(parent.getJobProfileName());
      futures.add(jobExecutionDao.updateJobExecution(child));
    }
    return CompositeFuture.all(futures).map(Future::succeeded);
  }

}
