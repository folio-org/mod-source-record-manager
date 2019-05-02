package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.WorkerExecutor;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.HttpStatus;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.client.SourceStorageClient;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.Progress;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RunBy;
import org.folio.rest.jaxrs.model.StatusDto;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.folio.rest.RestVerticle.MODULE_SPECIFIC_ARGS;

@Service
public class ChunkProcessingServiceImpl implements ChunkProcessingService {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChunkProcessingServiceImpl.class);

  private static final int THREAD_POOL_SIZE =
    Integer.parseInt(MODULE_SPECIFIC_ARGS.getOrDefault("update.records.thread.pool.size", "100"));
  private Vertx vertx;
  private JobExecutionSourceChunkDao jobExecutionSourceChunkDao;
  private JobExecutionService jobExecutionService;
  private ChangeEngineService changeEngineService;
  private AfterProcessingService afterProcessingService;
  private AdditionalFieldsConfig additionalFieldsConfig;
  private WorkerExecutor workerExecutor;

  public ChunkProcessingServiceImpl(@Autowired Vertx vertx,
                                    @Autowired JobExecutionSourceChunkDao jobExecutionSourceChunkDao,
                                    @Autowired JobExecutionService jobExecutionService,
                                    @Autowired ChangeEngineService changeEngineService,
                                    @Autowired AfterProcessingService afterProcessingService,
                                    @Autowired AdditionalFieldsConfig additionalFieldsConfig) {
    this.jobExecutionSourceChunkDao = jobExecutionSourceChunkDao;
    this.jobExecutionService = jobExecutionService;
    this.changeEngineService = changeEngineService;
    this.afterProcessingService = afterProcessingService;
    this.additionalFieldsConfig = additionalFieldsConfig;
    this.workerExecutor =
      this.vertx.createSharedWorkerExecutor("updating-records-thread-pool", THREAD_POOL_SIZE);
  }

  @Override
  public Future<Boolean> processChunk(RawRecordsDto chunk, String jobExecutionId, OkapiConnectionParams params) {
    JobExecutionSourceChunk sourceChunk = new JobExecutionSourceChunk()
      .withId(UUID.randomUUID().toString())
      .withJobExecutionId(jobExecutionId)
      .withLast(chunk.getLast())
      .withState(JobExecutionSourceChunk.State.IN_PROGRESS)
      .withChunkSize(chunk.getRecords().size())
      .withCreatedDate(new Date());
    return jobExecutionSourceChunkDao.save(sourceChunk, params.getTenantId())
      .compose(s -> checkAndUpdateJobExecutionStatusIfNecessary(jobExecutionId, JobExecution.Status.PARSING_IN_PROGRESS, params))
      .compose(e -> checkAndUpdateJobExecutionFieldsIfNecessary(jobExecutionId, params))
      .compose(jobExec -> changeEngineService.parseRawRecordsChunkForJobExecution(chunk, jobExec, sourceChunk.getId(), params))
      .compose(parsedRecords -> {
        postProcessRecords(parsedRecords, sourceChunk, jobExecutionId, params);
        return Future.succeededFuture(true);
      });
  }

  /**
   * Performs records processing after records were parsed
   *
   * @param parsedRecords  list of parsed records
   * @param sourceChunk    chunk of raw records
   * @param jobExecutionId job id
   * @param params         parameters enough to connect to OKAPI
   */
  private void postProcessRecords(List<Record> parsedRecords, JobExecutionSourceChunk sourceChunk, String jobExecutionId, OkapiConnectionParams params) {
    workerExecutor.executeBlocking(blockingFuture -> {
        RecordProcessingContext context = new RecordProcessingContext(parsedRecords);
        afterProcessingService.process(context, sourceChunk.getId(), params)
          .compose(r -> {
            sourceChunk.withState(JobExecutionSourceChunk.State.COMPLETED);
            return jobExecutionSourceChunkDao.update(sourceChunk, params.getTenantId());
          })
          .compose(ch -> checkIfProcessingCompleted(jobExecutionId, params.getTenantId()))
          .compose(completed -> {
            if (completed) {
              updateRecordsWithAdditionalFields(context, params);
              return checkAndUpdateJobExecutionStatusIfNecessary(jobExecutionId, JobExecution.Status.PARSING_FINISHED, params)
                .map(result -> true);
            }
            return Future.succeededFuture(true);
          });
      },
      false,
      null);
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
            .map(list -> list.stream().filter(chunk -> JobExecutionSourceChunk.State.COMPLETED.equals(chunk.getState())).count() == list.size());
        }
        return Future.succeededFuture(false);
      });
  }

  /**
   * @param processingContext context object with records and properties
   * @param params            OKAPI connection params
   */
  private void updateRecordsWithAdditionalFields(RecordProcessingContext processingContext, OkapiConnectionParams params) {
    if (!processingContext.getRecordsContext().isEmpty()) {
      RecordProcessingContext.RecordContext recordContext = processingContext.getRecordsContext().get(0);
      if (Record.RecordType.MARC.equals(recordContext.getRecord().getRecordType())) {
        addAdditionalFieldsToMarcRecords(processingContext, params);
      }
    }
  }

  /**
   * Adds additional custom fields to parsed MARC records
   * @param processingContext context object with records and properties
   * @param params            OKAPI connection params
   */
  private void addAdditionalFieldsToMarcRecords(RecordProcessingContext processingContext, OkapiConnectionParams params) {
    SourceStorageClient client = new SourceStorageClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    for (RecordProcessingContext.RecordContext recordContext : processingContext.getRecordsContext()) {
      addAdditionalFieldsToMarcRecord(recordContext);
      Record record = recordContext.getRecord();
      try {
        client.putSourceStorageRecordsById(record.getId(), null, record, response -> {
          if (response.statusCode() != HttpStatus.HTTP_OK.toInt()) {
            LOGGER.error("Error updating Record by id {}", record.getId());
          } else {
            LOGGER.info("Record with id {} successfully updated.", record.getId());
          }
        });
      } catch (Exception e) {
        LOGGER.error("Couldn't update Record with id {}", record.getId(), e);
      }
    }
  }

  /**
   * Adds additional custom fields to parsed MARC record
   * @param recordContext context object with record and properties
   */
  private void addAdditionalFieldsToMarcRecord(RecordProcessingContext.RecordContext recordContext) {
    Record record = recordContext.getRecord();
    JsonObject parsedRecordContent = new JsonObject(record.getParsedRecord().getContent().toString());
    if (parsedRecordContent.containsKey("fields")) {
      JsonArray fields = parsedRecordContent.getJsonArray("fields");
      String targetField = additionalFieldsConfig.apply(AdditionalFieldsConfig.TAG_999, content -> content
        .replace("{recordId}", record.getId())
        .replace("{instanceId}", recordContext.getInstanceId()));
      fields.add(new JsonObject(targetField));
      record.getParsedRecord().setContent(parsedRecordContent.toString());
    }
  }
}
