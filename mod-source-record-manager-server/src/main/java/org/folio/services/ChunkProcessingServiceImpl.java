package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.Progress;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RunBy;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.afterprocessing.AfterProcessingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.folio.rest.jaxrs.model.JobExecutionSourceChunk.State.COMPLETED;

@Service
public class ChunkProcessingServiceImpl implements ChunkProcessingService {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChunkProcessingServiceImpl.class);

  private Vertx vertx;
  private JobExecutionSourceChunkDao jobExecutionSourceChunkDao;
  private JobExecutionService jobExecutionService;
  private ChangeEngineService changeEngineService;
  private AfterProcessingService instanceProcessingService;

  public ChunkProcessingServiceImpl(@Autowired Vertx vertx,
                                    @Autowired JobExecutionSourceChunkDao jobExecutionSourceChunkDao,
                                    @Autowired JobExecutionService jobExecutionService,
                                    @Autowired ChangeEngineService changeEngineService,
                                    @Autowired AfterProcessingService instanceProcessingService) {
    this.vertx = vertx;
    this.jobExecutionSourceChunkDao = jobExecutionSourceChunkDao;
    this.jobExecutionService = jobExecutionService;
    this.changeEngineService = changeEngineService;
    this.instanceProcessingService = instanceProcessingService;
  }

  @Override
  public Future<Boolean> processChunk(RawRecordsDto incomingChunk, String jobExecutionId, OkapiConnectionParams params) {
    JobExecutionSourceChunk sourceChunk = new JobExecutionSourceChunk()
      .withId(UUID.randomUUID().toString())
      .withJobExecutionId(jobExecutionId)
      .withLast(incomingChunk.getLast())
      .withState(JobExecutionSourceChunk.State.IN_PROGRESS)
      .withChunkSize(incomingChunk.getRecords().size())
      .withCreatedDate(new Date());
    return jobExecutionSourceChunkDao.save(sourceChunk, params.getTenantId())
      .compose(s -> checkAndUpdateJobExecutionStatusIfNecessary(jobExecutionId, JobExecution.Status.PARSING_IN_PROGRESS, params))
      .compose(e -> checkAndUpdateJobExecutionFieldsIfNecessary(jobExecutionId, params))
      .compose(jobExec -> changeEngineService.parseRawRecordsChunkForJobExecution(incomingChunk, jobExec, sourceChunk.getId(), params))
      .compose(records -> postProcessRecords(records, sourceChunk, params))
      .compose(ar -> jobExecutionSourceChunkDao.update(sourceChunk
        .withState(JobExecutionSourceChunk.State.COMPLETED)
        .withCompletedDate(new Date()), params.getTenantId()))
      .compose(ch -> checkIfProcessingCompleted(jobExecutionId, params.getTenantId()))
      .compose(completed -> {
        if (completed) {
          // status should be JobExecution.Status.PARSING_FINISHED but for first version we finish import in this place
          return checkAndUpdateJobExecutionStatusIfNecessary(jobExecutionId, JobExecution.Status.COMMITTED, params)
            .map(result -> true);
        }
        return Future.succeededFuture(true);
      });
  }

  /**
   * Checks JobExecution current status and updates it if needed
   *
   * @param jobExecutionId - JobExecution id
   * @param status         - required status of JobExecution
   * @param params         - okapi connection params
   * @return future
   */
  private Future<JobExecution> checkAndUpdateJobExecutionStatusIfNecessary(String jobExecutionId, JobExecution.Status status, OkapiConnectionParams params) {
    return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
      .compose(optionalJobExecution -> optionalJobExecution
        .map(jobExecution -> {
          if (!status.equals(jobExecution.getStatus())) {
            return jobExecutionService.updateJobExecutionStatus(jobExecutionId, new StatusDto().withStatus(StatusDto.Status.fromValue(status.name())), params);
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
  private Future<JobExecution> checkAndUpdateJobExecutionFieldsIfNecessary(String jobExecutionId, OkapiConnectionParams params) {
    return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
      .compose(optionalJobExecution -> optionalJobExecution
        .map(jobExecution -> {
          if (jobExecution.getRunBy() == null || jobExecution.getProgress() == null || jobExecution.getStartedDate() == null) {
            return jobExecutionService.updateJobExecution(jobExecution
              .withRunBy(new RunBy().withFirstName("DIKU").withLastName("ADMINISTRATOR"))
              .withProgress(new Progress().withCurrent(1000).withTotal(1000))
              .withStartedDate(new Date()), params);
          }
          return Future.succeededFuture(jobExecution);
        }).orElse(Future.failedFuture(new NotFoundException(String.format("Couldn't find JobExecution with id %s", jobExecutionId)))));
  }

  /**
   * Checks if last chunk exists and if so checks that all chunks are processed
   *
   * @param jobExecutionId - JobExecution id
   * @return future with true if processing is completed, false if not
   */
  private Future<Boolean> checkIfProcessingCompleted(String jobExecutionId, String tenantId) {
    return jobExecutionSourceChunkDao.get("jobExecutionId=" + jobExecutionId + " AND last=true", 0, 1, tenantId)
      .compose(chunks -> {
        if (chunks != null && !chunks.isEmpty()) {
          return jobExecutionSourceChunkDao.get("jobExecutionId=" + jobExecutionId, 0, Integer.MAX_VALUE, tenantId)
            .map(list -> list.stream().filter(chunk -> COMPLETED.equals(chunk.getState())).count() == list.size());
        }
        return Future.succeededFuture(false);
      });
  }

  /**
   * Applies additional logic for already parsed records
   *
   * @param records     - target parsed records
   * @param sourceChunk - source chunk
   * @param params      - OkapiConnectionParams to interact with external services
   */
  private Future<Void> postProcessRecords(List<Record> records, JobExecutionSourceChunk sourceChunk, OkapiConnectionParams params) {
    Future<Void> future = Future.future();
    vertx.executeBlocking(blockingFuture ->
        instanceProcessingService.process(records, sourceChunk.getId(), params)
          .setHandler(ar -> {
            if (ar.failed()) {
              String errorMessage = String.format("Fail to complete blocking future for post processing records {}", ar.cause());
              LOGGER.error(errorMessage);
              blockingFuture.fail(errorMessage);
            } else {
              blockingFuture.complete();
            }
          })
      ,
      false,
      future.completer());
    return future;
  }
}
