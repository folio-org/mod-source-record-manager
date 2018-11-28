package org.folio.services.parsers;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.marc4j.MarcError;
import org.marc4j.MarcJsonWriter;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamReader;
import org.marc4j.marc.Record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Source record parser implementation for MARC format. Use marc4j library
 */
public final class MarcRecordParser implements SourceRecordParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(MarcRecordParser.class);

  @Override
  public ParsedResult parseRecord(String sourceRecord) {
    ParsedResult result = new ParsedResult();
    try {
      MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(sourceRecord.getBytes(StandardCharsets.UTF_8)));
      if (reader.hasNext()) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        MarcJsonWriter writer = new MarcJsonWriter(os);
        Record record = reader.next();
        List<MarcError> errorList = record.getErrors();
        if (errorList.isEmpty()) {
          writer.write(record);
          result.setParsedRecord(new JsonObject(new String(os.toByteArray())));
        } else {
          JsonObject errorObject = new JsonObject();
          JsonArray errors = new JsonArray();
          errorObject.put("errors", errors);
          errorList.forEach(e -> errors.add(buildErrorObject(e)));
          result.setErrors(errorObject);
          result.setHasError(true);
        }
        return result;
      }
    } catch (Exception e) {
      LOGGER.error("Error during parse MARC record from source record", e);
      result.setHasError(true);
    }
    return result;
  }

  /**
   * Build json representation of MarcRecord
   *
   * @param error - MarcRecord
   * @return - JsonObject with error descriptions
   */
  private JsonObject buildErrorObject(MarcError error) {
    JsonObject errorJson = new JsonObject();
    errorJson.put("message", error.message);
    errorJson.put("curField", error.curField);
    errorJson.put("curSubfield", error.curSubfield);
    return errorJson;
  }

  @Override
  public RecordFormat getParserFormat() {
    return RecordFormat.MARC;
  }
}
