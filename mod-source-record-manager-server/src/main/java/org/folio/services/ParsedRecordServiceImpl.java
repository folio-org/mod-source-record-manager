package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.util.Date;
import java.util.HashMap;
import java.util.UUID;
import javax.ws.rs.NotFoundException;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.Try;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.client.DataImportProfilesClient;
import org.folio.rest.client.SourceStorageClient;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.services.mappers.processor.MappingParametersProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static java.lang.String.format;
import static org.folio.HttpStatus.HTTP_CREATED;
import static org.folio.HttpStatus.HTTP_NOT_FOUND;
import static org.folio.HttpStatus.HTTP_OK;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.util.pubsub.PubSubClientUtils.constructModuleName;
import static org.folio.util.pubsub.PubSubClientUtils.sendEventMessage;

@Service
public class ParsedRecordServiceImpl implements ParsedRecordService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParsedRecordServiceImpl.class);

  private static final String DEFAULT_JOB_PROFILE_ID = "22fafcc3-f582-493d-88b0-3c538480cd83";
  private static final String QM_RECORD_UPDATED_EVENT_TYPE = "QM_SRS_MARC_BIB_RECORD_UPDATED";

  private MappingParametersProvider mappingParametersProvider;
  private MappingRuleService mappingRuleService;

  public ParsedRecordServiceImpl(@Autowired MappingParametersProvider mappingParametersProvider,
                                 @Autowired MappingRuleService mappingRuleService) {
    this.mappingParametersProvider = mappingParametersProvider;
    this.mappingRuleService = mappingRuleService;
  }

  @Override
  public Future<ParsedRecordDto> getRecordByInstanceId(String instanceId, OkapiConnectionParams params) {
    Promise<ParsedRecordDto> promise = Promise.promise();
    SourceStorageClient client = new SourceStorageClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.getSourceStorageSourceRecordsById(instanceId, "INSTANCE", response -> {
        if (HTTP_OK.toInt() == response.statusCode()) {
          response.bodyHandler(body -> promise.handle(Try.itGet(() -> mapSourceRecordToParsedRecordDto(body))));
        } else {
          String message = format("Error retrieving Record by instanceId: '%s', response code %s, %s",
            instanceId, response.statusCode(), response.statusMessage());
          if (HTTP_NOT_FOUND.toInt() == response.statusCode()) {
            promise.fail(new NotFoundException(message));
          } else {
            promise.fail(message);
          }
        }
      });
    } catch (Exception e) {
      LOGGER.error("Failed to GET Record from SRS", e);
      promise.fail(e);
    }
    return promise.future();
  }

  @Override
  public Future<Boolean> updateRecord(ParsedRecordDto parsedRecordDto, OkapiConnectionParams params) {
    return getRecordById(parsedRecordDto.getId(), params)
      .compose(oldRecord -> createSnapshot(params)
        .compose(snapshot -> createNewRecord(snapshot, oldRecord, parsedRecordDto, params))
        .compose(newRecord -> {
          sendEventWithUpdatedRecord(newRecord, params);
          return updateRecord(oldRecord.withState(Record.State.OLD), params)
            .map(ar -> true);
        }));
  }

  private ParsedRecordDto mapSourceRecordToParsedRecordDto(Buffer body) {
    SourceRecord sourceRecord = body.toJsonObject().mapTo(SourceRecord.class);
    return new ParsedRecordDto()
      .withId(sourceRecord.getRecordId())
      .withParsedRecord(sourceRecord.getParsedRecord())
      .withRecordType(ParsedRecordDto.RecordType.fromValue(sourceRecord.getRecordType().value()))
      .withExternalIdsHolder(sourceRecord.getExternalIdsHolder())
      .withAdditionalInfo(sourceRecord.getAdditionalInfo())
      .withMetadata(sourceRecord.getMetadata());
  }

  private Future<Record> getRecordById(String id, OkapiConnectionParams params) {
    Promise<Record> promise = Promise.promise();
    SourceStorageClient client = new SourceStorageClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.getSourceStorageRecordsById(id, null, response -> {
        if (HTTP_OK.toInt() == response.statusCode()) {
          response.bodyHandler(body -> promise.handle(Try.itGet(() -> body.toJsonObject().mapTo(Record.class))));
        } else {
          String message = format("Error retrieving Record by id: '%s', response code %s, %s",
            id, response.statusCode(), response.statusMessage());
          if (HTTP_NOT_FOUND.toInt() == response.statusCode()) {
            promise.fail(new NotFoundException(message));
          } else {
            promise.fail(message);
          }
        }
      });
    } catch (Exception e) {
      LOGGER.error("Failed to GET Record from SRS", e);
      promise.fail(e);
    }
    return promise.future();
  }

  private Future<Snapshot> createSnapshot(OkapiConnectionParams params) {
    Snapshot newSnapshot = new Snapshot()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withProcessingStartedDate(new Date()) // required for calculation of the generation value in SRS
      .withStatus(Snapshot.Status.COMMITTED); // no processing of the record is performed apart from the update itself
    return saveSnapshot(newSnapshot, params);
  }

  private Future<Record> createNewRecord(Snapshot snapshot, Record oldRecord, ParsedRecordDto parsedRecordDto, OkapiConnectionParams params) {
    Record newRecord = new Record()
      .withId(UUID.randomUUID().toString())
      .withSnapshotId(snapshot.getJobExecutionId())
      .withMatchedId(parsedRecordDto.getId())
      .withRecordType(Record.RecordType.fromValue(parsedRecordDto.getRecordType().value()))
      .withParsedRecord(parsedRecordDto.getParsedRecord())
      .withExternalIdsHolder(parsedRecordDto.getExternalIdsHolder())
      .withAdditionalInfo(parsedRecordDto.getAdditionalInfo())
      .withMetadata(parsedRecordDto.getMetadata())
      .withRawRecord(oldRecord.getRawRecord())
      .withOrder(oldRecord.getOrder())
      .withState(Record.State.ACTUAL);
    return saveRecord(newRecord, params);
  }

  private Future<Snapshot> saveSnapshot(Snapshot snapshot, OkapiConnectionParams params) {
    Promise<Snapshot> promise = Promise.promise();
    SourceStorageClient client = new SourceStorageClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.postSourceStorageSnapshots(null, snapshot, response -> {
        if (HTTP_CREATED.toInt() == response.statusCode()) {
          response.bodyHandler(body -> promise.handle(Try.itGet(() -> body.toJsonObject().mapTo(Snapshot.class))));
        } else {
          String message = format("Error saving Snapshot - response code %s, %s", response.statusCode(), response.statusMessage());
          LOGGER.error(message);
          promise.fail(message);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Failed to save Snapshot in SRS", e);
      promise.fail(e);
    }
    return promise.future();
  }

  private Future<Record> saveRecord(Record record, OkapiConnectionParams params) {
    Promise<Record> promise = Promise.promise();
    SourceStorageClient client = new SourceStorageClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.postSourceStorageRecords(null, record, response -> {
        if (HTTP_CREATED.toInt() == response.statusCode()) {
          response.bodyHandler(body -> promise.handle(Try.itGet(() -> body.toJsonObject().mapTo(Record.class))));
        } else {
          String message = format("Error saving Record - response code %s, %s", response.statusCode(), response.statusMessage());
          LOGGER.error(message);
          promise.fail(message);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Failed to save Record in SRS", e);
      promise.fail(e);
    }
    return promise.future();
  }

  private Future<Record> updateRecord(Record record, OkapiConnectionParams params) {
    Promise<Record> promise = Promise.promise();
    SourceStorageClient client = new SourceStorageClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.putSourceStorageRecordsById(record.getId(), null, record, response -> {
        if (HTTP_OK.toInt() == response.statusCode()) {
          response.bodyHandler(body -> promise.handle(Try.itGet(() -> body.toJsonObject().mapTo(Record.class))));
        } else {
          String message = format("Error updating Record - response code %s, %s", response.statusCode(), response.statusMessage());
          LOGGER.error(message);
          promise.fail(message);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Failed to update Record in SRS", e);
      promise.fail(e);
    }
    return promise.future();
  }

  private Future<Void> sendEventWithUpdatedRecord(Record record, OkapiConnectionParams params) {
    return mappingParametersProvider.get(record.getSnapshotId(), params)
      .compose(mappingParameters -> mappingRuleService.get(params.getTenantId())
        .compose(rulesOptional -> {
          if (rulesOptional.isPresent()) {
            return createJobProfileSnapshotWrapper(DEFAULT_JOB_PROFILE_ID, params)
              .compose(profileSnapshotWrapper -> sendEvent(record, profileSnapshotWrapper, rulesOptional.get(), mappingParameters, params));
          } else {
            return Future.failedFuture(format("Can not send %s event, no mapping rules found for tenant %s", QM_RECORD_UPDATED_EVENT_TYPE, params.getTenantId()));
          }
        }));
  }

  private Future<ProfileSnapshotWrapper> createJobProfileSnapshotWrapper(String jobProfileId, OkapiConnectionParams params) {
    Promise<ProfileSnapshotWrapper> promise = Promise.promise();
    DataImportProfilesClient client = new DataImportProfilesClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());

    client.postDataImportProfilesJobProfileSnapshotsById(jobProfileId, response -> {
      if (response.statusCode() == HTTP_CREATED.toInt()) {
        response.bodyHandler(body ->
          promise.handle(Try.itGet(() -> body.toJsonObject().mapTo(ProfileSnapshotWrapper.class))));
      } else {
        String message = format("Failed to create ProfileSnapshotWrapper by JobProfile id %s, response code %s, %s",
          jobProfileId, response.statusCode(), response.statusMessage());
        LOGGER.error(message);
        promise.fail(message);
      }
    });
    return promise.future();
  }

  private Future<Void> sendEvent(Record updatedRecord, ProfileSnapshotWrapper profileSnapshotWrapper,
                                 JsonObject mappingRules, MappingParameters mappingParameters, OkapiConnectionParams params) {
    Promise<Void> promise = Promise.promise();
    HashMap<String, String> dataImportEventPayloadContext = new HashMap<>();
    dataImportEventPayloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(updatedRecord));
    dataImportEventPayloadContext.put("MAPPING_RULES", mappingRules.encode());
    dataImportEventPayloadContext.put("MAPPING_PARAMS", Json.encode(mappingParameters));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(QM_RECORD_UPDATED_EVENT_TYPE)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withJobExecutionId(updatedRecord.getSnapshotId())
      .withContext(dataImportEventPayloadContext)
      .withOkapiUrl(params.getOkapiUrl())
      .withTenant(params.getTenantId())
      .withToken(params.getToken());

    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(QM_RECORD_UPDATED_EVENT_TYPE)
      .withEventPayload(Json.encode(dataImportEventPayload))
      .withEventMetadata(new EventMetadata()
        .withTenantId(params.getTenantId())
        .withEventTTL(1)
        .withPublishedBy(constructModuleName()));

    org.folio.rest.util.OkapiConnectionParams connectionParams = new org.folio.rest.util.OkapiConnectionParams();
    connectionParams.setOkapiUrl(params.getOkapiUrl());
    connectionParams.setToken(params.getToken());
    connectionParams.setTenantId(params.getTenantId());
    connectionParams.setVertx(params.getVertx());

    sendEventMessage(event, connectionParams)
      .whenComplete((ar, throwable) -> {
        if (throwable == null) {
          promise.complete();
        } else {
          LOGGER.error("Error during event sending: {}", throwable, event);
          promise.fail(throwable);
        }
      });
    return promise.future();
  }

}
