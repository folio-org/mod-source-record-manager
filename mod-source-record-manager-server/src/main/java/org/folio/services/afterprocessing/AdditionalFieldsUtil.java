package org.folio.services.afterprocessing;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.rest.jaxrs.model.Record;
import org.springframework.stereotype.Component;

/**
 * Util to work with additional fields
 */
@Component
public class AdditionalFieldsUtil {

  /**
   * Adds inventory instance id into MARC record
   *
   * @param record     record
   * @param instanceId UUID of Instance entity
   */
  public Future<Record> addInstanceIdToMarcRecord(Record record, String instanceId) {
    if (record.getParsedRecord() != null && record.getParsedRecord().getContent() != null) {
      JsonObject parsedRecordContent = new JsonObject(record.getParsedRecord().getContent().toString());
      if (parsedRecordContent.containsKey("fields")) {
        JsonArray fields = parsedRecordContent.getJsonArray("fields");
        for (int i = fields.size(); i-- > 0; ) {
          JsonObject targetField = fields.getJsonObject(i);
          if (targetField.containsKey(AdditionalFieldsConfig.TAG_999)) {
            JsonObject instanceIdSubField = new JsonObject().put("i", instanceId);
            targetField.getJsonObject(AdditionalFieldsConfig.TAG_999).getJsonArray("subfields").add(instanceIdSubField);
            record.getParsedRecord().setContent(parsedRecordContent.toString());
            break;
          }
        }
      }
    }
    return Future.succeededFuture(record);
  }
}
