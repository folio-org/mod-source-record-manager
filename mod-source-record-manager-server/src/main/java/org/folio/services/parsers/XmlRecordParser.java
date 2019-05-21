package org.folio.services.parsers;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.rest.jaxrs.model.RawRecordsDto.ContentType;
import org.marc4j.MarcJsonWriter;
import org.marc4j.MarcXmlReader;
import org.marc4j.marc.Record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Record parser implementation for records in MARC XML format. Uses marc4j library
 */
public class XmlRecordParser implements RecordParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(XmlRecordParser.class);

  @Override
  public ParsedResult parseRecord(String rawRecord) {
    ParsedResult result = new ParsedResult();
    try {
      MarcXmlReader reader = new MarcXmlReader(new ByteArrayInputStream(rawRecord.getBytes(StandardCharsets.UTF_8)));
      if (reader.hasNext()) {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        MarcJsonWriter writer = new MarcJsonWriter(os);
        Record record = reader.next();
        writer.write(record);
        result.setParsedRecord(new JsonObject(new String(os.toByteArray())));
      } else {
        result.setParsedRecord(new JsonObject());
      }
    } catch (Exception e) {
      LOGGER.error("Error during parse MARC record from MARC XML data", e);
      result.setHasError(true);
      result.setErrors(new JsonObject()
        .put("name", e.getClass().getName())
        .put("message", e.getMessage()));
    }
    return result;
  }

  @Override
  public ContentType getParserFormat() {
    return ContentType.MARC_XML;
  }
}
