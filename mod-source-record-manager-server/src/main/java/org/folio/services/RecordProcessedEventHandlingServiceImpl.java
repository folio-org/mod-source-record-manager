package org.folio.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionProgress;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.Progress;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.journal.JournalRecordMapperException;
import org.folio.services.journal.JournalService;
import org.folio.services.journal.JournalUtil;
import org.folio.services.progress.JobExecutionProgressService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import static java.lang.String.format;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_ITEM_UPDATED;
import static org.folio.rest.jaxrs.model.JobExecution.Status.COMMITTED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.COMPLETED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.ERROR;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.UPDATE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.ITEM;

@Service
public class RecordProcessedEventHandlingServiceImpl implements EventHandlingService {

  private static final Logger LOGGER = LogManager.getLogger();
  public static final String FAILED_EVENT_KEY = "FAILED_EVENT";
  public static final String ERROR_KEY = "ERROR_MSG";

  private JobExecutionProgressService jobExecutionProgressService;
  private JobExecutionService jobExecutionService;
  private JournalService journalService;


  public RecordProcessedEventHandlingServiceImpl(@Autowired JobExecutionProgressService jobExecutionProgressService,
                                                 @Autowired JobExecutionService jobExecutionService,
                                                 @Autowired @Qualifier("journalServiceProxy") JournalService journalService) {
    this.jobExecutionProgressService = jobExecutionProgressService;
    this.jobExecutionService = jobExecutionService;
    this.journalService = journalService;
  }

  @Override
  public Future<Boolean> handle(String eventContent, OkapiConnectionParams params) {
    Promise<Boolean> promise = Promise.promise();
    DataImportEventPayload dataImportEventPayload;
    try {
      dataImportEventPayload = new ObjectMapper().readValue(ZIPArchiver.unzip(eventContent), DataImportEventPayload.class);
    } catch (IOException e) {
      LOGGER.error("Failed to unzip event {}", eventContent, e);
      promise.fail(e);
      return promise.future();
    }

    String jobExecutionId = dataImportEventPayload.getJobExecutionId();
    try {
      DataImportEventTypes eventType = DataImportEventTypes.valueOf(dataImportEventPayload.getEventType());
      jobExecutionProgressService.updateJobExecutionProgress(jobExecutionId, progress -> changeProgressAccordingToEventType(progress, eventType), params.getTenantId())
        .compose(updatedProgress -> updateJobExecutionIfAllRecordsProcessed(jobExecutionId, updatedProgress, params))
        .onComplete(ar -> {
          if (ar.failed()) {
            LOGGER.error("Failed to handle {} event", eventType,  ar.cause());
            updateJobStatusToError(jobExecutionId, params).onComplete(statusAr -> promise.fail(ar.cause()));
          } else {
            promise.complete(true);
          }
        });
    } catch (Exception e) {
      LOGGER.error("Failed to handle event {}", eventContent, e);
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
            JobExecution.Status statusToUpdate = progress.getCurrentlyFailed() == 0 ? COMMITTED : JobExecution.Status.ERROR;
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
