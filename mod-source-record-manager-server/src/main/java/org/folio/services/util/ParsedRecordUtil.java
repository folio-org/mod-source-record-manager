package org.folio.services.util;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.SPACE;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import org.folio.rest.jaxrs.model.ParsedRecord;

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
    JsonArray fields = getFields(parsedRecord);
    if (fields == null) {
      return EMPTY;
    }

    return getSubfieldsStream(fieldTag, fields)
      .filter(subfield -> subfieldCodes.stream().anyMatch(subfield::containsKey))
      .map(ParsedRecordUtil::getSubfieldData)
      .collect(Collectors.joining(SUBFIELDS_DATA_DELIMITER));
  }

  /**
   * Retrieve data from all subfields of specified marc field.
   * Subfields data returns as a concatenated string in which they are separated by space character.
   *
   * @param parsedRecord parsed marc record
   * @param fieldTag     marc field tag
   * @return data from all subfields concatenated and separated by space character
   */
  public static String retrieveDataByField(ParsedRecord parsedRecord, String fieldTag) {
    JsonArray fields = getFields(parsedRecord);
    if (fields == null) {
      return EMPTY;
    }

    return getSubfieldsStream(fieldTag, fields)
      .map(ParsedRecordUtil::getSubfieldData)
      .collect(Collectors.joining(SUBFIELDS_DATA_DELIMITER));
  }

  private static String getSubfieldData(JsonObject subfield) {
    return subfield.iterator().next().getValue().toString();
  }

  private static Stream<JsonObject> getSubfieldsStream(String fieldTag, JsonArray fields) {
    return fields.stream()
      .map(JsonObject.class::cast)
      .filter(field -> field.containsKey(fieldTag))
      .flatMap(targetField -> targetField.getJsonObject(fieldTag).getJsonArray("subfields").stream())
      .map(subfieldAsObject -> (JsonObject) subfieldAsObject);
  }

  private static JsonArray getFields(ParsedRecord parsedRecord) {
    JsonObject parsedContent = new JsonObject(parsedRecord.getContent().toString());
    return parsedContent.getJsonArray("fields");
  }
}
