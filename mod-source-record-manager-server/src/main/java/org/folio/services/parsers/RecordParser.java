package org.folio.services.parsers;

import java.util.List;

import org.folio.rest.jaxrs.model.RecordsMetadata;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Common interface for Record Parser. Parsers for each format should implement it
 */
public interface RecordParser {

  /**
   * Method parses record and return String with json representation of record
   *
   * @param rawRecord - String with raw record
   * @return - Wrapper for parsed record in json format.
   * Can contains errors descriptions if parsing was failed
   */
  ParsedResult parseRecord(String rawRecord);

  /**
   * @return - format which RecordParser can parse
   */
  RecordsMetadata.ContentType getParserFormat();

  default void prepareResultWithError(ParsedResult result, List<JsonObject> errorObjects) {
    JsonObject errorObject = new JsonObject();
    JsonArray errors = new JsonArray();
    errorObject.put("errors", errors);
    errorObjects.forEach(errors::add);
    result.setErrors(errorObject);
    result.setHasError(true);
  }

}
