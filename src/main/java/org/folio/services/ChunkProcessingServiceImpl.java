package org.folio.services;

import io.vertx.core.Future;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.StatusDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.Date;
import java.util.UUID;

@Service
public class ChunkProcessingServiceImpl implements ChunkProcessingService {

  @Autowired
  private JobExecutionSourceChunkDao jobExecutionSourceChunkDao;
  @Autowired
  private JobExecutionService jobExecutionService;
  @Autowired
  private ChangeEngineService changeEngineService;

  @Override
  public Future<Boolean> processChunk(RawRecordsDto chunk, String jobExecutionId, OkapiConnectionParams params) {
    JobExecutionSourceChunk jobExecutionSourceChunk = new JobExecutionSourceChunk()
      .withId(UUID.randomUUID().toString())
      .withJobExecutionId(jobExecutionId)
      .withLast(chunk.getLast())
      .withState(JobExecutionSourceChunk.State.IN_PROGRESS)
      .withChunkSize(chunk.getRecords().size())
      .withCreatedDate(new Date());
    return jobExecutionSourceChunkDao.save(jobExecutionSourceChunk, params.getTenantId())
      .compose(s -> checkAndUpdateJobExecutionStatusIfNecessary(jobExecutionId, JobExecution.Status.IMPORT_IN_PROGRESS, params))
      .compose(jobExec -> changeEngineService.parseRawRecordsChunkForJobExecution(chunk, jobExec, params))
      .compose(rawRecords -> jobExecutionSourceChunkDao.update(jobExecutionSourceChunk
        .withState(JobExecutionSourceChunk.State.COMPLETED)
        .withCompletedDate(new Date()), params.getTenantId()))
      .compose(ch -> checkIfProcessingCompleted(jobExecutionId, params.getTenantId()))
      .compose(completed -> {
        if (completed) {
          return checkAndUpdateJobExecutionStatusIfNecessary(jobExecutionId, JobExecution.Status.IMPORT_FINISHED, params)
            .compose(this::processRecords)
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
