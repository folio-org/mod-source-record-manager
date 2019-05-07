package org.folio.services.afterProcessing;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.HttpStatus;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.client.SourceStorageClient;
import org.folio.rest.jaxrs.model.Record;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("additionalFieldsProcessingService")
public class AdditionalFieldsProcessingServiceImpl implements AfterProcessingService {
  private static final Logger LOGGER = LoggerFactory.getLogger(AdditionalFieldsProcessingServiceImpl.class);
  private AdditionalFieldsConfig additionalFieldsConfig;

  public AdditionalFieldsProcessingServiceImpl(@Autowired AdditionalFieldsConfig additionalFieldsConfig) {
    this.additionalFieldsConfig = additionalFieldsConfig;
  }

  @Override
  public Future<RecordProcessingContext> process(RecordProcessingContext context, String sourceChunkId, OkapiConnectionParams params) {
    if (!context.getRecordsContext().isEmpty()) {
      Record.RecordType recordType = getRecordsType(context);
      if (Record.RecordType.MARC.equals(recordType)) {
        addAdditionalFieldsToMarcRecords(context, params);
      }
    }
    return Future.succeededFuture(context);
  }

  /**
   * Gets content type of parsed records in the context
   *
   * @param context shared object to hold records and their related properties
   * @return content type of parsed records in the context
   */
  private Record.RecordType getRecordsType(RecordProcessingContext context) {
    return context.getRecordsContext().get(0).getRecord().getRecordType();
  }

  /**
   * Puts additional external fields to parsed MARC records from processing context
   *
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
   * Adds additional external fields to parsed MARC record from processing context
   *
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
