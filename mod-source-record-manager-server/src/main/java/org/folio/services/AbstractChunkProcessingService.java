package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.StatusDto;

import javax.ws.rs.NotFoundException;
import java.util.Date;
import java.util.UUID;

public abstract class AbstractChunkProcessingService implements ChunkProcessingService {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractChunkProcessingService.class);

  protected JobExecutionSourceChunkDao jobExecutionSourceChunkDao;
  protected JobExecutionService jobExecutionService;

  public AbstractChunkProcessingService(JobExecutionSourceChunkDao jobExecutionSourceChunkDao,
                                        JobExecutionService jobExecutionService) {
    this.jobExecutionSourceChunkDao = jobExecutionSourceChunkDao;
    this.jobExecutionService = jobExecutionService;
  }

  @Override
  public Future<Boolean> processChunk(RawRecordsDto incomingChunk, String jobExecutionId, OkapiConnectionParams params) {
    JobExecutionSourceChunk sourceChunk = new JobExecutionSourceChunk()
      .withId(UUID.randomUUID().toString())
      .withJobExecutionId(jobExecutionId)
      .withLast(incomingChunk.getRecordsMetadata().getLast())
      .withState(JobExecutionSourceChunk.State.IN_PROGRESS)
      .withChunkSize(incomingChunk.getInitialRecords().size())
      .withCreatedDate(new Date());

    LOGGER.debug("JobExecutionSourceChunk created");
    return jobExecutionSourceChunkDao.save(sourceChunk, params.getTenantId())
      .compose(records -> processRawRecordsChunk(incomingChunk, sourceChunk, jobExecutionId, params));
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
          if (!status.getStatus().value().equals(jobExecution.getStatus().value())) {
            return jobExecutionService.updateJobExecutionStatus(jobExecutionId, status, params);
          }
          return Future.succeededFuture(jobExecution);
        }).orElse(Future.failedFuture(new NotFoundException(String.format("Couldn't find JobExecution with id %s", jobExecutionId)))));
  }

}
