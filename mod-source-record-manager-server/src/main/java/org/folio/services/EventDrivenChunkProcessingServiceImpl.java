package org.folio.services;

import io.vertx.core.Future;

import lombok.extern.log4j.Log4j2;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.progress.JobExecutionProgressService;
import org.folio.verticle.consumers.util.JobExecutionUtils;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;

import static org.folio.rest.jaxrs.model.StatusDto.Status.PARSING_IN_PROGRESS;

@Log4j2
@Service("eventDrivenChunkProcessingService")
public class EventDrivenChunkProcessingServiceImpl extends AbstractChunkProcessingService {

  private final ChangeEngineService changeEngineService;
  private final JobExecutionProgressService jobExecutionProgressService;

  public EventDrivenChunkProcessingServiceImpl(@Autowired JobExecutionSourceChunkDao jobExecutionSourceChunkDao,
                                               @Autowired JobExecutionService jobExecutionService,
                                               @Autowired ChangeEngineService changeEngineService,
                                               @Autowired JobExecutionProgressService jobExecutionProgressService) {
    super(jobExecutionSourceChunkDao, jobExecutionService);
    this.changeEngineService = changeEngineService;
    this.jobExecutionProgressService = jobExecutionProgressService;
  }

  @Override
  protected Future<Boolean> processRawRecordsChunk(RawRecordsDto incomingChunk, JobExecutionSourceChunk sourceChunk, String jobExecutionId, boolean acceptInstanceId, ConnectionParams params) {
    log.debug("processRawRecordsChunk:: Starting to process raw records chunk with id: {} for jobExecutionId: {}. Chunk size: {}.", sourceChunk.getId(), jobExecutionId, sourceChunk.getChunkSize());
    return initializeJobExecutionProgressIfNecessary(jobExecutionId, incomingChunk, params.getTenantId())
      .compose(ar -> checkAndUpdateJobExecutionStatusIfNecessary(jobExecutionId, new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS), params))
      .compose(jobExec -> changeEngineService.parseRawRecordsChunkForJobExecution(incomingChunk, jobExec, sourceChunk.getId(), acceptInstanceId, params))
      .map(true);
  }

  /**
   * Processes a chunk received through the REST API (single-record imports: mod-copycat, consortium instance sharing).
   * Unlike the Kafka flow, no error handler reports a failed chunk as DI_ERROR events, and the callers stop at the
   * first error without sending the final chunk, so a failure would leave the job in progress forever.
   * Therefore a failure moves the source chunk and the job straight to ERROR.
   * <p>
   * The Kafka flow ({@link #processChunk(RawRecordsDto, JobExecution, ConnectionParams)}) must not do this: there a
   * failed chunk is reported per record by the error handler and the job completes by progress, while ending the job
   * early would make the consumers skip the chunks that are still in flight.
   */
  @Override
  public Future<Boolean> processChunk(RawRecordsDto incomingChunk, String jobExecutionId, boolean acceptInstanceId,
                                      ConnectionParams params) {
    return super.processChunk(incomingChunk, jobExecutionId, acceptInstanceId, params)
      .recover(e -> handleRestChunkFailure(incomingChunk, jobExecutionId, params, e)
        .transform(ignored -> Future.failedFuture(e)));
  }

  private Future<Void> handleRestChunkFailure(RawRecordsDto chunk, String jobExecutionId, ConnectionParams params,
                                              Throwable failure) {
    // a duplicate was already processed; the empty final chunk carries no record and the job finishes by progress
    if (failure instanceof DuplicateEventException || isEmptyLastChunk(chunk)) {
      return Future.succeededFuture();
    }
    String tenantId = params.getTenantId();
    return SourceChunkStateUpdater.markAsError(jobExecutionSourceChunkDao, chunk.getId(), jobExecutionId, tenantId)
      .compose(v -> jobExecutionService.getJobExecutionById(jobExecutionId, tenantId))
      .compose(jobExecution -> jobExecution
        // do not overwrite a job that was cancelled by the user or has already failed
        .filter(job -> !JobExecutionUtils.SKIP_STATUSES.contains(job.getStatus()))
        .map(job -> jobExecutionService.updateJobExecutionStatus(jobExecutionId, new StatusDto()
            .withStatus(StatusDto.Status.ERROR)
            .withErrorStatus(StatusDto.ErrorStatus.RECORD_UPDATE_ERROR), params)
          .<Void>mapEmpty())
        .orElseGet(Future::succeededFuture))
      .onFailure(e -> log.warn("handleRestChunkFailure:: Failed to mark jobExecution {} as ERROR", jobExecutionId, e));
  }

  private static boolean isEmptyLastChunk(RawRecordsDto chunk) {
    return Boolean.TRUE.equals(chunk.getRecordsMetadata().getLast()) && CollectionUtils.isEmpty(chunk.getInitialRecords());
  }

  private Future<Boolean> initializeJobExecutionProgressIfNecessary(String jobExecutionId, RawRecordsDto incomingChunk, String tenantId) {
    return jobExecutionService.getJobExecutionById(jobExecutionId, tenantId)
      .compose(optionalJobExecution -> optionalJobExecution
        .map(jobExecution -> {
          JobExecution.Status jobStatus = jobExecution.getStatus();
          if (PARSING_IN_PROGRESS.value().equals(jobStatus.value()) || StatusDto.Status.ERROR.value().equals(jobStatus.value())
            || StatusDto.Status.CANCELLED.value().equals(jobStatus.value())) {
            return Future.succeededFuture(true);
          }
          return jobExecutionProgressService.initializeJobExecutionProgress(jobExecution.getId(), incomingChunk.getRecordsMetadata().getTotal(), tenantId).map(true);
        }).orElse(Future.failedFuture(new NotFoundException(String.format("Couldn't find JobExecution with id %s", jobExecutionId)))));
  }
}
