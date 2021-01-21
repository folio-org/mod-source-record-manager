package org.folio.services.parsers;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.xlate.edi.stream.EDIInputFactory;
import io.xlate.edi.stream.EDIStreamException;
import io.xlate.edi.stream.EDIStreamReader;

import org.folio.rest.jaxrs.model.RecordsMetadata;
import org.marc4j.MarcError;
import org.marc4j.MarcJsonWriter;
import org.marc4j.MarcReader;
import org.marc4j.MarcStreamReader;
import org.marc4j.marc.Record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Raw record parser implementation for MARC format. Use marc4j library
 */
public final class EDIFACTRecordParser implements RecordParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(EDIFACTRecordParser.class);
  // private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

  @Override
  public ParsedResult parseRecord(String rawRecord) {
    ParsedResult result = new ParsedResult();
    EDIInputFactory factory = EDIInputFactory.newFactory();
    InputStream stream = new ByteArrayInputStream(rawRecord.getBytes());
    EDIStreamReader reader = factory.createEDIStreamReader(stream);

    try {
      while (reader.hasNext()) {




      }
      reader.close();
      stream.close();
    } catch (EDIStreamException | IOException e) {
      LOGGER.error("Error during parse MARC record from raw record", e);
      prepareResultWithError(result, Collections.singletonList(new JsonObject()
        .put("name", e.getClass().getName())
        .put("message", e.getMessage())));
    }

    

    try {
      // MarcReader reader = new MarcStreamReader(new ByteArrayInputStream(rawRecord.getBytes(DEFAULT_CHARSET)), DEFAULT_CHARSET.name());
      // if (reader.hasNext()) {
      //   ByteArrayOutputStream os = new ByteArrayOutputStream();
      //   MarcJsonWriter writer = new MarcJsonWriter(os);
      //   Record record = reader.next();
      //   List<MarcError> errorList = record.getErrors();
      //   if (errorList == null || errorList.isEmpty()) {
      //     writer.write(record);
      //     result.setParsedRecord(new JsonObject(new String(os.toByteArray())));
      //   } else {
      //     List<JsonObject> preparedErrors = new ArrayList<>();
      //     errorList.forEach(e -> preparedErrors.add(buildErrorObject(e)));
      //     prepareResultWithError(result, preparedErrors);
      //   }
      //   return result;
      // } else {
        // result.setParsedRecord(new JsonObject());
      // }
    } catch (Exception e) {
      // LOGGER.error("Error during parse MARC record from raw record", e);
      // prepareResultWithError(result, Collections.singletonList(new JsonObject()
      //   .put("name", e.getClass().getName())
      //   .put("message", e.getMessage())));
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

  private void prepareResultWithError(ParsedResult result, List<JsonObject> errorObjects) {
    JsonObject errorObject = new JsonObject();
    JsonArray errors = new JsonArray();
    errorObject.put("errors", errors);
    errorObjects.forEach(errors::add);
    result.setErrors(errorObject);
    result.setHasError(true);
  }

  @Override
  public RecordsMetadata.ContentType getParserFormat() {
    return RecordsMetadata.ContentType.EDIFACT_RAW;
  }
}
