package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionProgress;
import org.folio.rest.jaxrs.model.Progress;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.services.progress.JobExecutionProgressService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;

import static java.lang.String.format;
import static org.folio.rest.jaxrs.model.JobExecution.Status.COMMITTED;
import static org.folio.rest.jaxrs.model.JobExecution.Status.ERROR;

@Service
public class RecordProcessedEventHandlingServiceImpl implements EventHandlingService {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordProcessedEventHandlingServiceImpl.class);

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
    try {
      DataImportEventPayload dataImportEventPayload = ObjectMapperTool.getMapper().readValue(eventContent, DataImportEventPayload.class);
      String jobExecutionId = dataImportEventPayload.getJobExecutionId();
      DataImportEventTypes eventType = DataImportEventTypes.valueOf(dataImportEventPayload.getEventType());
      jobExecutionProgressService.updateJobExecutionProgress(jobExecutionId, progress -> changeProgressAccordingToEventType(progress, eventType), params.getTenantId())
        .compose(updatedProgress -> updateJobExecutionIfAllRecordsProcessed(jobExecutionId, updatedProgress, params))
        .setHandler(ar -> {
          if (ar.failed()) {
            LOGGER.error("Failed to handle {} event", ar.cause(), eventType);
            updateJobStatusToError(jobExecutionId, params).setHandler(statusAr -> {
              promise.fail(ar.cause());
            });
          } else {
            promise.complete(true);
          }
        });
    } catch (Exception e) {
      LOGGER.error("Failed to handle event {}", e, eventContent);
      promise.fail(e);
    }
    return promise.future();
  }

  private Future<JobExecution> updateJobStatusToError(String jobExecutionId, OkapiConnectionParams params) {
    return jobExecutionService.updateJobExecutionStatus(jobExecutionId, new StatusDto()
      .withStatus(StatusDto.Status.ERROR)
      .withErrorStatus(StatusDto.ErrorStatus.FILE_PROCESSING_ERROR), params);
  }

  private JobExecutionProgress changeProgressAccordingToEventType(JobExecutionProgress progress, DataImportEventTypes eventType) {
    switch (eventType) {
      case DI_COMPLETED:
        return progress.withCurrentlySucceeded(progress.getCurrentlySucceeded() + 1);
      case DI_ERROR:
        return progress.withCurrentlyFailed(progress.getCurrentlyFailed() + 1);
      default:
        LOGGER.error("Illegal event type specified '{}' ", eventType);
        return progress;
    }
  }

  private Future<Boolean> updateJobExecutionIfAllRecordsProcessed(String jobExecutionId, JobExecutionProgress progress, OkapiConnectionParams params) {
    if (progress.getTotal().equals(progress.getCurrentlySucceeded() + progress.getCurrentlyFailed())) {
      return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
        .compose(jobExecutionOptional -> jobExecutionOptional
          .map(jobExecution -> {
            JobExecution.Status statusToUpdate = progress.getCurrentlyFailed() == 0 ? COMMITTED : ERROR;
            jobExecution.withStatus(statusToUpdate)
              .withUiStatus(JobExecution.UiStatus.fromValue(Status.valueOf(statusToUpdate.name()).getUiStatus()))
              .withCompletedDate(new Date())
              .withProgress(new Progress()
                .withJobExecutionId(jobExecutionId)
                .withCurrent(progress.getCurrentlySucceeded() + progress.getCurrentlyFailed())
                .withTotal(progress.getTotal()));

            return jobExecutionService.updateJobExecutionWithSnapshotStatus(jobExecution, params).map(true);
          })
          .orElse(Future.failedFuture(format("Couldn't find JobExecution for update status and progress with id '%s'", jobExecutionId))));
    }
    return Future.succeededFuture(false);
  }
}
