package org.folio.services.parsers;

import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.folio.rest.jaxrs.model.RecordsMetadata;
import org.marc4j.MarcJsonWriter;
import org.marc4j.MarcXmlReader;
import org.marc4j.marc.Record;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Record parser implementation for records in MARC XML format. Uses marc4j library
 */
@Log4j2
public class XmlRecordParser implements RecordParser {

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
      log.warn("parseRecord:: Error during parse MARC record from MARC XML data", e);
      result.setErrors(new JsonObject()
        .put("name", e.getClass().getName())
        .put("message", e.getMessage()));
    }
    return result;
  }

  @Override
  public RecordsMetadata.ContentType getParserFormat() {
    return RecordsMetadata.ContentType.MARC_XML;
  }
}
