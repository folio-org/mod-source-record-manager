package org.folio.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
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
import static org.folio.rest.jaxrs.model.JobExecution.Status.ERROR;

@Service
public class RecordProcessedEventHandlingServiceImpl implements EventHandlingService {

  private static final Logger LOGGER = LoggerFactory.getLogger(RecordProcessedEventHandlingServiceImpl.class);
  public static final String FAILED_EVENT_KEY = "FAILED_EVENT";
  public static final String ERROR_KEY = "ERROR_MSG";

  private JobExecutionProgressService jobExecutionProgressService;
  private JobExecutionService jobExecutionService;
  private JournalService journalService;


  public RecordProcessedEventHandlingServiceImpl(@Autowired JobExecutionProgressService jobExecutionProgressService,
                                                 @Autowired JobExecutionService jobExecutionService,
                                                 @Autowired @Qualifier(value = "journalServiceProxy") JournalService journalService) {
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
      LOGGER.error("Failed to unzip event {}", e, eventContent);
      promise.fail(e);
      return promise.future();
    }

    String jobExecutionId = dataImportEventPayload.getJobExecutionId();
    try {
      DataImportEventTypes eventType = DataImportEventTypes.valueOf(dataImportEventPayload.getEventType());
      jobExecutionProgressService.updateJobExecutionProgress(jobExecutionId, progress -> changeProgressAccordingToEventType(progress, eventType), params.getTenantId())
        .onSuccess(ar -> saveJournalRecordIfNecessary(dataImportEventPayload))
        .compose(updatedProgress -> updateJobExecutionIfAllRecordsProcessed(jobExecutionId, updatedProgress, params))
        .onComplete(ar -> {
          if (ar.failed()) {
            LOGGER.error("Failed to handle {} event", ar.cause(), eventType);
            updateJobStatusToError(jobExecutionId, params).onComplete(statusAr -> promise.fail(ar.cause()));
          } else {
            promise.complete(true);
          }
        });
    } catch (Exception e) {
      LOGGER.error("Failed to handle event {}", e, eventContent);
      updateJobStatusToError(jobExecutionId, params);
      promise.fail(e);
    }
    return promise.future();
  }

  private void saveJournalRecordIfNecessary(DataImportEventPayload dataImportEventPayload) {
    String lastEvent = retrieveLastResultEvent(dataImportEventPayload);
    if (lastEvent != null && DataImportEventTypes.valueOf(lastEvent) == DI_INVENTORY_ITEM_UPDATED) {
      saveJournalRecord(dataImportEventPayload);
    }
  }

  private String retrieveLastResultEvent(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getEventType().equals(DI_ERROR.value())) {
      return dataImportEventPayload.getContext().get(FAILED_EVENT_KEY);
    }
    List<String> eventsChain = dataImportEventPayload.getEventsChain();
    return eventsChain.size() > 0 ? eventsChain.get(eventsChain.size() - 1) : null;
  }

  private void saveJournalRecord(DataImportEventPayload dataImportEventPayload) {
    try {
      String errorMessage = dataImportEventPayload.getContext().get(ERROR_KEY);

      JournalRecord journalRecord = dataImportEventPayload.getEventType().equals(DI_ERROR.value())
        ? JournalUtil.buildJournalRecord(dataImportEventPayload, JournalRecord.ActionType.UPDATE,
          JournalRecord.EntityType.ITEM, JournalRecord.ActionStatus.ERROR, errorMessage)
        : JournalUtil.buildJournalRecord(dataImportEventPayload, JournalRecord.ActionType.UPDATE,
          JournalRecord.EntityType.ITEM, JournalRecord.ActionStatus.COMPLETED);

      journalService.save(JsonObject.mapFrom(journalRecord), dataImportEventPayload.getTenant());
    } catch (JournalRecordMapperException e) {
      LOGGER.error("Error journal record saving for event payload {}", e, dataImportEventPayload);
      e.printStackTrace();
    }
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
