package org.folio.services.parsers;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.rest.jaxrs.model.RecordsMetadata;


/**
 * Json record parser implementation
 */
public class JsonRecordParser implements RecordParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(JsonRecordParser.class);

  @Override
  public ParsedResult parseRecord(String rawRecord) {
    ParsedResult result = new ParsedResult();
    try {
      result.setParsedRecord(new JsonObject(rawRecord));
    } catch (Exception e) {
      LOGGER.error("Error mapping parsed record to json", e);
      result.setHasError(true);
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
