package org.folio.services.parsers;

import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.folio.rest.jaxrs.model.RecordsMetadata;

/**
 * Json record parser implementation
 */
@Log4j2
public class JsonRecordParser implements RecordParser {

  @Override
  public ParsedResult parseRecord(String rawRecord) {
    ParsedResult result = new ParsedResult();
    try {
      result.setParsedRecord(new JsonObject(rawRecord));
    } catch (Exception e) {
      log.warn("parseRecord:: Error mapping parsed record to json", e);
      result.setErrors(new JsonObject()
        .put("message", e.getMessage())
        .put("error", rawRecord));
    }
    return result;
  }

  @Override
  public RecordsMetadata.ContentType getParserFormat() {
    return RecordsMetadata.ContentType.MARC_JSON;
  }
}
