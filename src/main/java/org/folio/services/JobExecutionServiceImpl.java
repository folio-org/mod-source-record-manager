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
import org.folio.services.converters.JobExecutionToDtoConverter;
import org.folio.util.OkapiConnectionParams;
import org.folio.util.RestUtil;

import javax.ws.rs.BadRequestException;
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

  public static final String SNAPSHOT_SERVICE_URL = "/source-storage/snapshot";
  private static final Logger logger = LoggerFactory.getLogger(JobExecutionServiceImpl.class);
  private JobExecutionDao jobExecutionDao;
  private JobExecutionToDtoConverter jobExecutionToDtoConverter;

  public JobExecutionServiceImpl(Vertx vertx, String tenantId) {
    this.jobExecutionDao = new JobExecutionDaoImpl(vertx, tenantId);
    this.jobExecutionToDtoConverter = new JobExecutionToDtoConverter();
  }

  public Future<JobExecutionCollectionDto> getCollectionDtoByQuery(String query, int offset, int limit) {
    return jobExecutionDao.getJobExecutions(query, offset, limit)
      .map(jobExecutionCollection -> new JobExecutionCollectionDto()
        .withJobExecutionDtos(jobExecutionToDtoConverter.convert(jobExecutionCollection.getJobExecutions()))
        .withTotalRecords(jobExecutionCollection.getTotalRecords()));
  }

  public Future<InitJobExecutionsRsDto> initializeJobExecutions(InitJobExecutionsRqDto jobExecutionsRqDto, OkapiConnectionParams params) {
    if (jobExecutionsRqDto.getFiles().isEmpty()) {
      logger.error("Received files must not be empty");
      return Future.failedFuture(new BadRequestException());
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

  private Future<List<String>> saveJobExecutions(List<JobExecution> jobExecutions) {
    List<Future> savedJobExecutionFutures = new ArrayList<>();;
    for (JobExecution jobExecution : jobExecutions) {
      Future<String> savedJobExecutionFuture = jobExecutionDao.save(jobExecution);
      savedJobExecutionFutures.add(savedJobExecutionFuture);
    }
    return CompositeFuture.all(savedJobExecutionFutures).map(compositeFuture -> compositeFuture.result().list());
  }

  private Future saveSnapshots(List<JsonObject> snapshots, OkapiConnectionParams params) {
    List<Future> postedSnapshotFutures = new ArrayList<>();
    for (JsonObject snapshot : snapshots) {
      Future<String> postedSnapshotFuture = postSnapshot(snapshot, params);
      postedSnapshotFutures.add(postedSnapshotFuture);
    }
    return CompositeFuture.all(postedSnapshotFutures).map(compositeFuture -> compositeFuture.result().list());
  }

  private Future<String> postSnapshot(JsonObject snapshot, OkapiConnectionParams params) {
    Future<String> future = Future.future();
    RestUtil.doRequest(params, SNAPSHOT_SERVICE_URL, HttpMethod.POST, snapshot.encode())
      .setHandler(responseResult -> {
        try {
          int responseCode = responseResult.result().getCode();
          if (responseResult.failed() || responseCode != CREATED_STATUS_CODE) {
            logger.error("Error during post for new Snapshot. Response code: " + responseCode, responseResult.cause());
            future.fail(responseResult.cause());
          } else {
            String responseBody = responseResult.result().getBody();
            future.complete(responseBody);
          }
        } catch (Exception e) {
          logger.error("Error during post for new Snapshot", e, e.getMessage());
          future.fail(e);
        }
      });
    return future;
  }
}
