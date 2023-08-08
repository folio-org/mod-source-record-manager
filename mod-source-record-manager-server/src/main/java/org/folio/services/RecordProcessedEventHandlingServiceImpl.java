package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionProgress;
import org.folio.rest.jaxrs.model.Progress;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.progress.JobExecutionProgressService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Date;

import static java.lang.String.format;
import static org.folio.rest.jaxrs.model.JobExecution.Status.CANCELLED;
import static org.folio.rest.jaxrs.model.JobExecution.Status.COMMITTED;

@Service
public class RecordProcessedEventHandlingServiceImpl implements EventHandlingService {

  private static final Logger LOGGER = LogManager.getLogger();
  public static final String ERROR_KEY = "ERROR";
  public static final String ERRORS_KEY = "ERRORS";
  private static final String EMPTY_ARRAY = "[]";

  private JobExecutionProgressService jobExecutionProgressService;
  private JobExecutionService jobExecutionService;

  public RecordProcessedEventHandlingServiceImpl(@Autowired JobExecutionProgressService jobExecutionProgressService,
                                                 @Autowired JobExecutionService jobExecutionService) {
    this.jobExecutionProgressService = jobExecutionProgressService;
    this.jobExecutionService = jobExecutionService;
  }

  @Override
  public Future<Boolean> handle(String eventContent, OkapiConnectionParams params) {
    Promise<Boolean> promise = Promise.promise();
    DataImportEventPayload dataImportEventPayload;
    try {
      dataImportEventPayload = Json.decodeValue(eventContent, DataImportEventPayload.class);
    } catch (DecodeException e) {
      LOGGER.warn("handle:: Failed to read eventContent {}", eventContent, e);
      promise.fail(e);
      return promise.future();
    }

    String jobExecutionId = dataImportEventPayload.getJobExecutionId();
    try {
      DataImportEventTypes eventType = DataImportEventTypes.valueOf(dataImportEventPayload.getEventType());
      int successCount = 0;
      int errorCount = 0;
      if (DataImportEventTypes.DI_COMPLETED.equals(eventType)) {
        JsonArray errors = new JsonArray(dataImportEventPayload.getContext().get(ERRORS_KEY) != null ?
          dataImportEventPayload.getContext().get(ERRORS_KEY) : EMPTY_ARRAY);
        if (!errors.isEmpty()) {
          errorCount++;
        } else {
          successCount++;
        }
      } else if (DataImportEventTypes.DI_ERROR.equals(eventType)) {
        errorCount++;
      } else {
        LOGGER.warn("handle:: Illegal event type specified '{}' ", eventType);
        return Future.succeededFuture(false);
      }

      jobExecutionProgressService.updateCompletionCounts(jobExecutionId, successCount, errorCount, params.getTenantId())
        .compose(updatedProgress -> updateJobExecutionIfAllRecordsProcessed(jobExecutionId, updatedProgress, params))
        .onComplete(ar -> {
          if (ar.failed()) {
            LOGGER.warn("handle:: Failed to handle {} event", eventType, ar.cause());
            updateJobStatusToError(jobExecutionId, params)
              .onComplete(statusAr -> promise.fail(ar.cause()));
          } else {
            promise.complete(true);
          }
        });
    } catch (Exception e) {
      LOGGER.warn("handle:: Failed to handle event {}", eventContent, e);
      updateJobStatusToError(jobExecutionId, params);
      promise.fail(e);
    }
    return promise.future();
  }

  private Future<JobExecution> updateJobStatusToError(String jobExecutionId, OkapiConnectionParams params) {
    return jobExecutionService.updateJobExecutionStatus(jobExecutionId, new StatusDto()
      .withStatus(StatusDto.Status.ERROR)
      .withErrorStatus(StatusDto.ErrorStatus.FILE_PROCESSING_ERROR), params);
  }

  private Future<Boolean> updateJobExecutionIfAllRecordsProcessed(String jobExecutionId, JobExecutionProgress progress, OkapiConnectionParams params) {
    if (progress.getTotal().equals(progress.getCurrentlySucceeded() + progress.getCurrentlyFailed())) {
      return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
        .compose(jobExecutionOptional -> jobExecutionOptional
          .map(jobExecution -> {
            JobExecution.Status statusToUpdate;
            if (jobExecution.getStatus() == CANCELLED && jobExecution.getUiStatus() == JobExecution.UiStatus.CANCELLED) {
              statusToUpdate = CANCELLED;
            } else if (progress.getCurrentlyFailed() == 0) {
              statusToUpdate = COMMITTED;
            } else {
              statusToUpdate = JobExecution.Status.ERROR;
            }
            jobExecution.withStatus(statusToUpdate)
              .withUiStatus(JobExecution.UiStatus.fromValue(Status.valueOf(statusToUpdate.name()).getUiStatus()))
              .withCompletedDate(new Date())
              .withProgress(new Progress()
                .withJobExecutionId(jobExecutionId)
                .withCurrent(progress.getCurrentlySucceeded() + progress.getCurrentlyFailed())
                .withTotal(progress.getTotal()));

            return jobExecutionService.updateJobExecutionWithSnapshotStatus(jobExecution, params)
              .map(true);
          })
          .orElse(Future.failedFuture(format("Couldn't find JobExecution for update status and progress with id '%s'", jobExecutionId))));
    }
    return Future.succeededFuture(false);
  }
}
