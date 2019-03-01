package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dao.JobExecutionSourceChunkDaoImpl;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.StatusDto;

import javax.ws.rs.NotFoundException;
import java.util.Date;
import java.util.UUID;

public class ChunkProcessingServiceImpl implements ChunkProcessingService {

  private JobExecutionSourceChunkDao jobExecutionSourceChunkDao;
  private JobExecutionService jobExecutionService;
  private ChangeEngineService changeEngineService;

  public ChunkProcessingServiceImpl(Vertx vertx, String tenantId) {
    this.jobExecutionSourceChunkDao = new JobExecutionSourceChunkDaoImpl(vertx, tenantId);
    this.jobExecutionService = new JobExecutionServiceImpl(vertx, tenantId);
    this.changeEngineService = new ChangeEngineServiceImpl(vertx, tenantId);
  }

  @Override
  public Future<Boolean> processChunk(RawRecordsDto chunk, String jobExecutionId, OkapiConnectionParams params) {
    JobExecutionSourceChunk jobExecutionSourceChunk = new JobExecutionSourceChunk()
      .withId(UUID.randomUUID().toString())
      .withJobExecutionId(jobExecutionId)
      .withLast(chunk.getLast())
      .withState(JobExecutionSourceChunk.State.IN_PROGRESS)
      .withChunkSize(chunk.getRecords().size())
      .withCreatedDate(new Date());
    return jobExecutionSourceChunkDao.save(jobExecutionSourceChunk)
      .compose(s -> checkAndUpdateJobExecutionStatusIfNecessary(jobExecutionId, JobExecution.Status.IMPORT_IN_PROGRESS, params))
      .compose(jobExec -> changeEngineService.parseRawRecordsChunkForJobExecution(chunk, jobExec, params))
      .compose(rawRecords -> jobExecutionSourceChunkDao.update(jobExecutionSourceChunk
        .withState(JobExecutionSourceChunk.State.COMPLETED)
        .withCompletedDate(new Date())))
      .compose(ch -> checkIfProcessingCompleted(jobExecutionId))
      .compose(completed -> {
        if (completed) {
          return checkAndUpdateJobExecutionStatusIfNecessary(jobExecutionId, JobExecution.Status.IMPORT_FINISHED, params)
            .compose(this::processRecords)
            .map(result -> true);
        }
        return Future.succeededFuture(true);
      });
  }

  private Future<JobExecution> checkAndUpdateJobExecutionStatusIfNecessary(String jobExecutionId, JobExecution.Status status, OkapiConnectionParams params) {
    return jobExecutionService.getJobExecutionById(jobExecutionId)
      .compose(optionalJobExecution -> optionalJobExecution
        .map(jobExecution -> {
          if (!status.equals(jobExecution.getStatus())) {
            return jobExecutionService.updateJobExecutionStatus(jobExecutionId, new StatusDto().withStatus(StatusDto.Status.fromValue(status.name())), params);
          }
          return Future.succeededFuture(jobExecution);
        }).orElse(Future.failedFuture(new NotFoundException(String.format("Couldn't find JobExecution with id %s", jobExecutionId)))));
  }

  private Future<Boolean> checkIfProcessingCompleted(String jobExecutionId) {
    return jobExecutionSourceChunkDao.get("jobExecutionId=" + jobExecutionId + " AND last=true",0, 1)
      .compose(chunks -> {
        if(chunks != null && !chunks.isEmpty()) {
          return jobExecutionSourceChunkDao.get("jobExecutionId=" + jobExecutionId, 0, Integer.MAX_VALUE)
            .map(list -> list.stream().filter(chunk -> JobExecutionSourceChunk.State.COMPLETED.equals(chunk.getState())).count() == list.size());
        }
        return Future.succeededFuture(false);
      });
  }

  /**
   * STUB implementation since processing of parsed records is not defined yet
   */
  private Future<JobExecution> processRecords(JobExecution jobExecution) {
    return Future.succeededFuture(jobExecution);
  }
}
