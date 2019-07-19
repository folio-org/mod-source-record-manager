package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.RestUtil;
import org.folio.rest.RestVerticle;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.Progress;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.RunBy;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.UserInfo;
import org.folio.services.afterprocessing.AfterProcessingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.Date;
import java.util.UUID;

import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;

@Service
public class ChunkProcessingServiceImpl implements ChunkProcessingService {
  private JobExecutionSourceChunkDao jobExecutionSourceChunkDao;
  private JobExecutionService jobExecutionService;
  private ChangeEngineService changeEngineService;
  private AfterProcessingService instanceProcessingService;

  private static final Logger LOGGER = LoggerFactory.getLogger(ChunkProcessingServiceImpl.class);
  private static final String GET_USER_URL = "/users?query=id==";

  public ChunkProcessingServiceImpl(@Autowired JobExecutionSourceChunkDao jobExecutionSourceChunkDao,
                                    @Autowired JobExecutionService jobExecutionService,
                                    @Autowired ChangeEngineService changeEngineService,
                                    @Autowired AfterProcessingService instanceProcessingService) {
    this.jobExecutionSourceChunkDao = jobExecutionSourceChunkDao;
    this.jobExecutionService = jobExecutionService;
    this.changeEngineService = changeEngineService;
    this.instanceProcessingService = instanceProcessingService;
  }

  @Override
  public Future<Boolean> processChunk(RawRecordsDto incomingChunk, String jobExecutionId, OkapiConnectionParams params) {
    Future<Boolean> future = Future.future();

    JobExecutionSourceChunk sourceChunk = new JobExecutionSourceChunk()
      .withId(UUID.randomUUID().toString())
      .withJobExecutionId(jobExecutionId)
      .withLast(incomingChunk.getRecordsMetadata().getLast())
      .withState(JobExecutionSourceChunk.State.IN_PROGRESS)
      .withChunkSize(incomingChunk.getRecords().size())
      .withCreatedDate(new Date());
    jobExecutionSourceChunkDao.save(sourceChunk, params.getTenantId())
      .compose(s -> checkAndUpdateJobExecutionStatusIfNecessary(jobExecutionId, new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS), params))
      .compose(progress -> checkAndUpdateJobExecutionFieldsIfNecessary(jobExecutionId, incomingChunk, params))
      .compose(jobExec -> changeEngineService.parseRawRecordsChunkForJobExecution(incomingChunk, jobExec, sourceChunk.getId(), params))
      .compose(records -> instanceProcessingService.process(records, sourceChunk.getId(), params))
      .setHandler(chunkProcessAr -> updateJobExecutionStatusIfAllChunksProcessed(jobExecutionId, params)
        .setHandler(jobUpdateAr -> future.handle(chunkProcessAr.map(true))));
    return future;
  }

  /**
   * Updates jobExecution by its id only when last chunk was processed.
   *
   * @param jobExecutionId jobExecution Id
   * @param params         Okapi connection params
   * @return future with true if last chunk was processed and jobExecution updated, otherwise future with false
   */
  private Future<Boolean> updateJobExecutionStatusIfAllChunksProcessed(String jobExecutionId, OkapiConnectionParams params) {
    return checkJobExecutionState(jobExecutionId, params.getTenantId())
      .compose(statusDto -> {
        if (StatusDto.Status.PARSING_IN_PROGRESS != statusDto.getStatus()) {
          return checkAndUpdateJobExecutionStatusIfNecessary(jobExecutionId, statusDto, params)
            .compose(jobExecution -> jobExecutionService.updateJobExecution(jobExecution.withCompletedDate(new Date()), params)
              .map(true));
        }
        return Future.succeededFuture(false);
      });
  }

  /**
   * Checks JobExecution current status and updates it if needed
   *
   * @param jobExecutionId - JobExecution id
   * @param status         - required statusDto of JobExecution
   * @param params         - okapi connection params
   * @return future
   */
  private Future<JobExecution> checkAndUpdateJobExecutionStatusIfNecessary(String jobExecutionId, StatusDto status, OkapiConnectionParams params) {
    return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
      .compose(optionalJobExecution -> optionalJobExecution
        .map(jobExecution -> {
          if (!status.getStatus().value().equals(jobExecution.getStatus().value())) {
            return jobExecutionService.updateJobExecutionStatus(jobExecutionId, status, params);
          }
          return Future.succeededFuture(jobExecution);
        }).orElse(Future.failedFuture(new NotFoundException(String.format("Couldn't find JobExecution with id %s", jobExecutionId)))));
  }

  /**
   * STUB implementation
   * Checks JobExecution runBy, progress and startedDate fields and updates them if needed
   *
   * @param jobExecutionId - JobExecution id
   * @param params         - okapi connection params
   * @return future
   */
  private Future<JobExecution> checkAndUpdateJobExecutionFieldsIfNecessary(String jobExecutionId, RawRecordsDto incomingChunk, OkapiConnectionParams params) {
    return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
      .compose(optionalJobExecution -> optionalJobExecution
        .map(jobExecution -> {
          Integer totalValue = incomingChunk.getRecordsMetadata().getTotal();
          Integer counterValue = incomingChunk.getRecordsMetadata().getCounter();
          Progress progress = new Progress()
            .withJobExecutionId(jobExecutionId)
            .withCurrent(incomingChunk.getRecordsMetadata().getCounter())
            .withTotal(totalValue != null ? totalValue : counterValue);
          jobExecution.setProgress(progress);

          if (jobExecution.getRunBy() == null) {
            String userId = params.getHeaders().get(RestVerticle.OKAPI_USERID_HEADER);
            lookupUser(userId, params)
              .setHandler(userResult -> {
                if(userResult.succeeded()){
                  UserInfo userInfo = userResult.result();
                  jobExecution.setRunBy(new RunBy());
                }
              });
            jobExecution
              .withRunBy(new RunBy().withFirstName("DIKU").withLastName("ADMINISTRATOR"))
              .withStartedDate(new Date());
          }

          return jobExecutionService.updateJobExecution(jobExecution, params);
        }).orElse(Future.failedFuture(new NotFoundException(String.format("Couldn't find JobExecution with id %s", jobExecutionId)))));
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
   * Checks actual status of JobExecution
   *
   * @param jobExecutionId - JobExecution id
   * @return future with actual JobExecution status
   */
  private Future<StatusDto> checkJobExecutionState(String jobExecutionId, String tenantId) {
    return jobExecutionSourceChunkDao.get("jobExecutionId=" + jobExecutionId + " AND last=true", 0, 1, tenantId)
      .compose(chunks -> isNotEmpty(chunks) ? jobExecutionSourceChunkDao.isAllChunksProcessed(jobExecutionId, tenantId) : Future.succeededFuture(false))
      .compose(completed -> {
        if (completed) {
          return jobExecutionSourceChunkDao.containsErrorChunks(jobExecutionId, tenantId)
            .compose(hasErrors -> Future.succeededFuture(hasErrors ?
              new StatusDto().withStatus(StatusDto.Status.ERROR).withErrorStatus(StatusDto.ErrorStatus.INSTANCE_CREATING_ERROR) :
              // status should be JobExecution.Status.PARSING_FINISHED but for first version we finish import in this place
              new StatusDto().withStatus(StatusDto.Status.COMMITTED)));
        }
        return Future.succeededFuture(new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS));
      });
  }
}
