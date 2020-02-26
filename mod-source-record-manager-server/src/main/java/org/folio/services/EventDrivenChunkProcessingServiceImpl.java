package org.folio.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.DataImportEventPayload;
import org.folio.ProfileSnapshotWrapper;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.tools.PomReader;
import org.folio.util.pubsub.PubSubClientUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
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
        .map(jobExecution -> {
          ProfileSnapshotWrapper profileSnapshotWrapper = new ObjectMapper().convertValue(jobExecution.getJobProfileSnapshotWrapper(), ProfileSnapshotWrapper.class);
          List<Future> futures = createdRecords.stream()
              .filter(this::isRecordReadyToSend)
              .map(record -> sendEventWithRecord(record, profileSnapshotWrapper, params))
              .collect(Collectors.toList());
            return CompositeFuture.join(futures).map(true);
        })
        .orElse(Future.failedFuture(new NotFoundException(format("Couldn't find JobExecution with id %s", jobExecutionId)))));
  }

  /**
   * Checks whether the record contains parsed content for sending.
   *
   * @param record record for verification
   * @return true if record has parsed content
   */
  private boolean isRecordReadyToSend(Record record) {
    if (record.getParsedRecord() == null || record.getParsedRecord().getContent() == null) {
      LOGGER.error("The record has not parsed content, so it will not be sent to mod-pubsub");
      return false;
    }
    return true;
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

    HashMap<String, String> dataImportEventPayloadContext = new HashMap<>();
    dataImportEventPayloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(createdRecord));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withContext(dataImportEventPayloadContext);

    Event createdRecordEvent = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withEventPayload(Json.encode(dataImportEventPayload))
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
