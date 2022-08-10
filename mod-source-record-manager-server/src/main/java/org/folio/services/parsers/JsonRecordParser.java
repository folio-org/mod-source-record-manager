package org.folio.services.parsers;

import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.jaxrs.model.RecordsMetadata;


/**
 * Json record parser implementation
 */
public class JsonRecordParser implements RecordParser {

  private static final Logger LOGGER = LogManager.getLogger();

  @Override
  public ParsedResult parseRecord(String rawRecord) {
    ParsedResult result = new ParsedResult();
    try {
      result.setParsedRecord(new JsonObject(rawRecord));
    } catch (Exception e) {
      LOGGER.error("Error mapping parsed record to json", e);
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
