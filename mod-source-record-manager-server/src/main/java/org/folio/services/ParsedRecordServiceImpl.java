package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.util.Date;
import java.util.UUID;
import javax.ws.rs.NotFoundException;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.Try;
import org.folio.rest.client.SourceStorageClient;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.springframework.stereotype.Service;

import static java.lang.String.format;
import static org.folio.HttpStatus.HTTP_CREATED;
import static org.folio.HttpStatus.HTTP_NOT_FOUND;
import static org.folio.HttpStatus.HTTP_OK;

@Service
public class ParsedRecordServiceImpl implements ParsedRecordService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParsedRecordServiceImpl.class);

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
        .compose(ar -> updateRecord(oldRecord.withState(Record.State.OLD), params))
        .map(ar -> true));
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

}
