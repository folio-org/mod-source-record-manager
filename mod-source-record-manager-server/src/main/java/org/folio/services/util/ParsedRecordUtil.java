package org.folio.services.util;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.rest.jaxrs.model.ParsedRecord;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.SPACE;

public final class ParsedRecordUtil {

  public static final String SUBFIELDS_DATA_DELIMITER = SPACE;

  private ParsedRecordUtil() {
  }

  /**
   * Retrieve data from specified subfields of specified marc field.
   * Subfields data returns as a concatenated string in which they are separated by space character.
   *
   * @param parsedRecord  parsed marc record
   * @param fieldTag      marc field tag
   * @param subfieldCodes subfields codes
   * @return data from specified subfields concatenated and separated by space character
   */
  public static String retrieveDataByField(ParsedRecord parsedRecord, String fieldTag, List<String> subfieldCodes) {
    JsonObject parsedContent = new JsonObject(parsedRecord.getContent().toString());
    JsonArray fields = parsedContent.getJsonArray("fields");
    if (fields == null) {
      return EMPTY;
    }

    return fields.stream()
      .map(JsonObject.class::cast)
      .filter(field -> field.containsKey(fieldTag))
      .flatMap(targetField -> targetField.getJsonObject(fieldTag).getJsonArray("subfields").stream())
      .map(subfieldAsObject -> (JsonObject) subfieldAsObject)
      .filter(subfield -> subfieldCodes.stream().anyMatch(subfieldCode -> subfield.containsKey(subfieldCode)))
      .map(subfield -> subfield.stream().findFirst().get().getValue().toString())
      .collect(Collectors.joining(SUBFIELDS_DATA_DELIMITER));
  }
}
