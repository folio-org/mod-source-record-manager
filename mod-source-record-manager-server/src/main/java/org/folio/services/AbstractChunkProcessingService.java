package org.folio.services;

import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.StatusDto;

import javax.ws.rs.NotFoundException;
import java.util.Date;


public abstract class AbstractChunkProcessingService implements ChunkProcessingService {
  private static final Logger LOGGER = LogManager.getLogger();
  private static final String JOB_EXECUTION_MARKED_AS_ERROR_MSG = "Couldn't update JobExecution status, JobExecution already marked as ERROR";

  protected JobExecutionSourceChunkDao jobExecutionSourceChunkDao;
  protected JobExecutionService jobExecutionService;

  public AbstractChunkProcessingService(JobExecutionSourceChunkDao jobExecutionSourceChunkDao,
                                        JobExecutionService jobExecutionService) {
    this.jobExecutionSourceChunkDao = jobExecutionSourceChunkDao;
    this.jobExecutionService = jobExecutionService;
  }

  @Override
  public Future<Boolean> processChunk(RawRecordsDto incomingChunk, String jobExecutionId, OkapiConnectionParams params) {
    prepareChunk(incomingChunk);
    return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
      .compose(optionalJobExecution -> optionalJobExecution
        .map(jobExecution -> {
          JobExecutionSourceChunk sourceChunk = new JobExecutionSourceChunk()
            .withId(incomingChunk.getId())
            .withJobExecutionId(jobExecutionId)
            .withLast(incomingChunk.getRecordsMetadata().getLast())
            .withState(JobExecutionSourceChunk.State.IN_PROGRESS)
            .withChunkSize(incomingChunk.getInitialRecords().size())
            .withCreatedDate(new Date());

          return jobExecutionSourceChunkDao.save(sourceChunk, params.getTenantId())
            .onSuccess(ar -> processRawRecordsChunk(incomingChunk, sourceChunk, jobExecution.getId(), params)).map(true)
            .onFailure(th -> Future.succeededFuture(false));
        }).orElse(Future.failedFuture(new NotFoundException(String.format("Couldn't find JobExecution with id %s", jobExecutionId)))));
  }

  private void prepareChunk(RawRecordsDto rawRecordsDto) {
    boolean isAnyRecordHasNoOrder = rawRecordsDto.getInitialRecords().stream()
      .anyMatch(initialRecord -> initialRecord.getOrder() == null);

    if (rawRecordsDto.getInitialRecords() != null && isAnyRecordHasNoOrder) {
      int firstRecordOrderOfCurrentChunk = rawRecordsDto.getRecordsMetadata().getCounter() - rawRecordsDto.getInitialRecords().size();

      for (InitialRecord initialRecord : rawRecordsDto.getInitialRecords()) {
        initialRecord.setOrder(firstRecordOrderOfCurrentChunk++);
      }
    }
  }

  /**
   * Process chunk of RawRecords
   *
   * @param incomingChunk  - chunk with raw records
   * @param sourceChunk    - source chunk job execution
   * @param jobExecutionId - JobExecution id
   * @param params         - okapi connection params
   * @return future with boolean
   */
  protected abstract Future<Boolean> processRawRecordsChunk(RawRecordsDto incomingChunk, JobExecutionSourceChunk sourceChunk, String jobExecutionId, OkapiConnectionParams params);

  /**
   * Checks JobExecution current status and updates it if needed
   *
   * @param jobExecutionId - JobExecution id
   * @param status         - required statusDto of JobExecution
   * @param params         - okapi connection params
   * @return future
   */
  protected Future<JobExecution> checkAndUpdateJobExecutionStatusIfNecessary(String jobExecutionId, StatusDto status, OkapiConnectionParams params) {
    return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
      .compose(optionalJobExecution -> optionalJobExecution
        .map(jobExecution -> {
          if (jobExecution.getStatus() == JobExecution.Status.ERROR) {
            LOGGER.error(JOB_EXECUTION_MARKED_AS_ERROR_MSG);
            return Future.<JobExecution>failedFuture(JOB_EXECUTION_MARKED_AS_ERROR_MSG);
          }
          if (jobExecution.getStatus() == JobExecution.Status.COMMITTED) {
            return Future.succeededFuture(jobExecution);
          }
          if (!status.getStatus().value().equals(jobExecution.getStatus().value())) {
            return jobExecutionService.updateJobExecutionStatus(jobExecutionId, status, params);
          }
          return Future.succeededFuture(jobExecution);
        }).orElse(Future.failedFuture(new NotFoundException(String.format("Couldn't find JobExecution with id %s", jobExecutionId)))));
  }

}
