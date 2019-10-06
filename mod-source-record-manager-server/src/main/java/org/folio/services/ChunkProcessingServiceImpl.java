package org.folio.services;

import io.vertx.core.Future;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.Progress;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.StatusDto;
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
      .withChunkSize(incomingChunk.getInitialRecords().size())
      .withCreatedDate(new Date());
    jobExecutionSourceChunkDao.save(sourceChunk, params.getTenantId())
      .compose(job -> updateJobExecutionProgress(jobExecutionId, incomingChunk, params))
      .compose(s -> checkAndUpdateJobExecutionStatusIfNecessary(jobExecutionId, new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS), params))
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
   * Updates JobExecution progress
   *
   * @param jobExecutionId - JobExecution id
   * @param params         - okapi connection params
   * @return future
   */
  private Future<JobExecution> updateJobExecutionProgress(String jobExecutionId, RawRecordsDto incomingChunk, OkapiConnectionParams params) {
    return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
      .compose(optionalJobExecution -> optionalJobExecution
        .map(jobExecution -> {
          Integer totalValue = incomingChunk.getRecordsMetadata().getTotal();
          Integer counterValue = incomingChunk.getRecordsMetadata().getCounter() != null ? incomingChunk.getRecordsMetadata().getCounter() : 1;
          Progress progress = new Progress()
            .withJobExecutionId(jobExecutionId)
            .withCurrent(counterValue)
            .withTotal(totalValue != null ? totalValue : counterValue);
          jobExecution.setProgress(progress);
          if (jobExecution.getStartedDate() == null) {
            jobExecution.setStartedDate(new Date());
          }
          return jobExecutionService.updateJobExecution(jobExecution, params);
        }).orElse(Future.failedFuture(new NotFoundException(String.format("Couldn't find JobExecution with id %s", jobExecutionId)))));
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
