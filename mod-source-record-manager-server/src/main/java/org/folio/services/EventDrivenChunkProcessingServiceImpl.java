package org.folio.services;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.ProfileSnapshotWrapper;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.events.model.EventContext;
import org.folio.rest.client.DataImportProfilesClient;
import org.folio.rest.jaxrs.model.*;
import org.folio.rest.tools.PomReader;
import org.folio.util.pubsub.PubSubClientUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.folio.HttpStatus.HTTP_OK;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.CREATED_SRS_MARC_BIB_RECORD;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;

@Service("eventDrivenChunkProcessingService")
public class EventDrivenChunkProcessingServiceImpl extends AbstractChunkProcessingService {
  private static final Logger LOGGER = LoggerFactory.getLogger(EventDrivenChunkProcessingServiceImpl.class);

  private ChangeEngineService changeEngineService;

  public EventDrivenChunkProcessingServiceImpl(@Autowired JobExecutionSourceChunkDao jobExecutionSourceChunkDao,
                                               @Autowired JobExecutionService jobExecutionService,
                                               @Autowired ChangeEngineService changeEngineService) {
    super(jobExecutionSourceChunkDao, jobExecutionService);
    this.changeEngineService = changeEngineService;
  }

  @Override
  protected Future<Boolean> processRawRecordsChunk(RawRecordsDto incomingChunk, JobExecutionSourceChunk sourceChunk, String jobExecutionId, OkapiConnectionParams params) {
    return checkAndUpdateJobExecutionStatusIfNecessary(jobExecutionId, new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS), params)
      .compose(jobExecution -> changeEngineService.parseRawRecordsChunkForJobExecution(incomingChunk, jobExecution, sourceChunk.getId(), params))
      .compose(records -> sendEventsWithCreatedRecords(records, jobExecutionId, params));
  }

  /**
   * Sends events with created records to the mod-pubsub
   *
   * @param createdRecords records to send
   * @param jobExecutionId job execution id
   * @param params         connection parameters
   * @return future with boolean
   */
  private Future<Boolean> sendEventsWithCreatedRecords(List<Record> createdRecords, String jobExecutionId, OkapiConnectionParams params) {
    return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
      .compose(jobOptional -> jobOptional
        .map(jobExecution -> getJobProfileSnapshotWrapper(jobExecution, params))
        .orElse(Future.failedFuture(new NotFoundException(format("Couldn't find JobExecution with id %s", jobExecutionId)))))
      .compose(profileSnapshotWrapper -> {
        List<Future> futures = createdRecords.stream()
          .filter(record -> record.getParsedRecord() != null && record.getParsedRecord().getContent() != null)
          .map(record -> sendEventWithRecord(record, profileSnapshotWrapper, params))
          .collect(Collectors.toList());
       return CompositeFuture.join(futures).map(true);
      });
  }

  /**
   * Returns ProfileSnapshotWrapper for specified jobExecution
   *
   * @param jobExecution jobExecution
   * @param params       okapi connection parameters
   * @return future with ProfileSnapshotWrapper
   */
  private Future<ProfileSnapshotWrapper> getJobProfileSnapshotWrapper(JobExecution jobExecution, OkapiConnectionParams params) {
    Promise<ProfileSnapshotWrapper> promise = Promise.promise();
    DataImportProfilesClient client = new DataImportProfilesClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());

    client.getDataImportProfilesJobProfileSnapshotsById(jobExecution.getJobProfileSnapshotWrapperId(), response -> {
      if (response.statusCode() == HTTP_OK.toInt()) {
        response.bodyHandler(body -> {
          try {
            promise.complete(body.toJsonObject().mapTo(ProfileSnapshotWrapper.class));
          } catch (Exception e) {
            String message = format("Error during deserializing ProfileSnapshotWrapper from response, cause: %s", e.getMessage());
            LOGGER.error(message, e);
            promise.fail(message);
          }
        });
      } else {
        String message = format("Error getting ProfileSnapshotWrapper by JobProfile id '%s', response code %s", jobExecution.getId(), response.statusCode());
        LOGGER.error(message);
        promise.fail(message);
      }
    });
    return promise.future();
  }

  /**
   * Prepares event with createdRecord, profileSnapshotWrapper and sends prepared event to the mod-pubsub
   *
   * @param createdRecord           record to send
   * @param profileSnapshotWrapper  profileSnapshotWrapper to send
   * @param params                  connection parameters
   * @return completed future with record if record was sent successfully
   */
  private Future<Record> sendEventWithRecord(Record createdRecord, ProfileSnapshotWrapper profileSnapshotWrapper, OkapiConnectionParams params) {
    Promise<Record> promise = Promise.promise();

    EventContext eventContext = new EventContext();
    eventContext.setEventType(CREATED_SRS_MARC_BIB_RECORD.value());
    eventContext.setProfileSnapshot(profileSnapshotWrapper);
    eventContext.setCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));
    eventContext.putObject(MARC_BIBLIOGRAPHIC.value(), Json.encode(createdRecord));

    Event createdRecordEvent = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(CREATED_SRS_MARC_BIB_RECORD.value())
      .withEventPayload(Json.encode(eventContext))
      .withEventMetadata(new EventMetadata()
        .withTenantId(params.getTenantId())
        .withEventTTL(1)
        .withPublishedBy(PomReader.INSTANCE.getModuleName() + "-" + PomReader.INSTANCE.getVersion()));

    org.folio.rest.util.OkapiConnectionParams connectionParams = new org.folio.rest.util.OkapiConnectionParams();
    connectionParams.setOkapiUrl(params.getOkapiUrl());
    connectionParams.setToken(params.getToken());
    connectionParams.setTenantId(params.getTenantId());
    connectionParams.setVertx(params.getVertx());

    PubSubClientUtils.sendEventMessage(createdRecordEvent, connectionParams)
      .whenComplete((ar, throwable) -> {
        if (throwable == null) {
          promise.complete(createdRecord);
        } else {
          LOGGER.error("Error during event sending: {}", throwable, createdRecordEvent);
          promise.fail(throwable);
        }
      });
    return promise.future();
  }
}
