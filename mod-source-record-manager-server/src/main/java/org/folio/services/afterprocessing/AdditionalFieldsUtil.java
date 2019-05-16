package org.folio.services.afterprocessing;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.HttpStatus;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.client.SourceStorageClient;
import org.folio.rest.jaxrs.model.Record;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Util to work with additional fields
 */
@Component
public class AdditionalFieldsUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(AdditionalFieldsUtil.class);

  /**
   * Adds additional fields to records from processing context
   *
   * @param context records processing context
   * @param params  okapi connection params
   */
  public Future<Void> addAdditionalFields(RecordProcessingContext context, OkapiConnectionParams params) {
    if (!context.getRecordsContext().isEmpty() && Record.RecordType.MARC.equals(context.getRecordsType())) {
       return addAdditionalFieldsToMarcRecords(context, params);
    }
    return Future.succeededFuture();
  }

  /**
   * Puts additional external fields to parsed MARC records from processing context
   *
   * @param processingContext context object with records and properties
   * @param params            OKAPI connection params
   */
  private Future<Void> addAdditionalFieldsToMarcRecords(RecordProcessingContext processingContext, OkapiConnectionParams params) {
    Future<Void> future = Future.future();
    SourceStorageClient client = new SourceStorageClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    List<Future> updatedRecordsFuture = new ArrayList<>();
    for (RecordProcessingContext.RecordContext recordContext : processingContext.getRecordsContext()) {
      updatedRecordsFuture.add(getRecordById(recordContext.getRecordId(), client)
        .compose(record -> putInstanceIdToMarcRecord(record, recordContext))
        .compose(record -> updateRecord(record, client)));
    }
    CompositeFuture.all(updatedRecordsFuture).setHandler(ar -> {
      if (ar.failed()) {
        LOGGER.error("Failed to add additional fields for MARC records", ar.cause());
        future.fail(ar.cause());
      } else {
        future.complete();
      }
    });
    return future;
  }

  /**
   * Return record by requested record id
   *
   * @param id     record id
   * @param client http client
   * @return record by requested record id
   */
  protected Future<Record> getRecordById(String id, SourceStorageClient client) {
    Future<Record> future = Future.future();
    try {
      client.getSourceStorageRecordsById(id, null, response -> {
        if (response.statusCode() == HttpStatus.HTTP_OK.toInt()) {
          response.bodyHandler(buffer -> future.complete(buffer.toJsonObject().mapTo(Record.class)));
        } else {
          String errorMessage = "Error getting record by id " + id;
          LOGGER.error(errorMessage);
          future.fail(errorMessage);
        }
      });
    } catch (Exception e) {
      String errorMessage = "Error sending request to get record by id " + id;
      LOGGER.error(errorMessage);
      future.fail(errorMessage);
    }
    return future;
  }

  /**
   * Adds inventory instance id into MARC record
   *
   * @param record        record
   * @param recordContext context object with record and properties
   */
  protected Future<Record> putInstanceIdToMarcRecord(Record record, RecordProcessingContext.RecordContext recordContext) {
    //TODO make sure there is no NPE
    JsonObject parsedRecordContent = JsonObject.mapFrom(new ObjectMapper().convertValue(record.getParsedRecord().getContent(), JsonObject.class));
    if (parsedRecordContent.containsKey("fields")) {
      JsonArray fields = parsedRecordContent.getJsonArray("fields");
      for (int i = fields.size(); i-- > 0; ) {
        JsonObject targetField = fields.getJsonObject(i);
        if (targetField.containsKey(AdditionalFieldsConfig.TAG_999)) {
          JsonObject instanceIdSubField = new JsonObject().put("i", recordContext.getInstanceId());
          targetField.getJsonObject(AdditionalFieldsConfig.TAG_999).getJsonArray("subfields").add(instanceIdSubField);
          record.getParsedRecord().setContent(parsedRecordContent.toString());
          break;
        }
      }
    }
    return Future.succeededFuture(record);
  }

  /**
   * Updates given record
   *
   * @param record given record
   * @param client http client
   * @return void
   */
  protected Future<Void> updateRecord(Record record, SourceStorageClient client) {
    Future<Void> future = Future.future();
    try {
      client.putSourceStorageRecordsById(record.getId(), null, record, response -> {
        if (response.statusCode() != HttpStatus.HTTP_OK.toInt()) {
          String errorMessage = "Error updating Record by id " + record.getId();
          LOGGER.error(errorMessage);
          future.fail(errorMessage);
        } else {
          future.complete();
        }
      });
    } catch (Exception e) {
      LOGGER.error("Couldn't send request to update Record with id {}", record.getId(), e);
      future.fail(e);
    }
    return future;
  }
}
