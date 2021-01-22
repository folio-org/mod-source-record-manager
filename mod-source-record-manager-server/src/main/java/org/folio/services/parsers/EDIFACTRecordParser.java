package org.folio.services.parsers;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import org.folio.rest.jaxrs.model.RecordsMetadata;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.xlate.edi.stream.EDIInputFactory;
import io.xlate.edi.stream.EDIStreamException;
import io.xlate.edi.stream.EDIStreamReader;

/**
 * Raw record parser implementation for MARC format. Use marc4j library
 */
public final class EdifactRecordParser implements RecordParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(EdifactRecordParser.class);

  private static final String SEGMENTS_LABEL = "segments";
  private static final String TAG_LABEL = "tag";
  private static final String DATA_ELEMENTS_LABEL = "dataElements";
  private static final String COMPONENTS_LABEL = "components";
  private static final String DATA_LABEL = "data";
  
  @Override
  public ParsedResult parseRecord(String rawRecord) {
    ParsedResult result = new ParsedResult();
    EDIInputFactory factory = EDIInputFactory.newFactory();
    InputStream stream = new ByteArrayInputStream(rawRecord.getBytes());
    EDIStreamReader reader = factory.createEDIStreamReader(stream);
    
    boolean buildingComposite = false;

    JsonObject resultJson = new JsonObject();
    JsonArray segmentsJson = new JsonArray();

    resultJson.put(SEGMENTS_LABEL, segmentsJson);

    try {
      while (reader.hasNext()) {
        switch (reader.next()) {
          case START_INTERCHANGE:
            break;
          case START_SEGMENT:
            String segmentName = reader.getText();
            JsonObject segmentJson = new JsonObject();
            segmentJson.put(TAG_LABEL, segmentName);
            segmentJson.put(DATA_ELEMENTS_LABEL, new JsonArray());
            segmentsJson.add(segmentJson);
            break;
          case END_SEGMENT:
            break;
          case START_COMPOSITE:
            buildingComposite = true;
            JsonObject compositeSegment = segmentsJson.getJsonObject(segmentsJson.size()-1);
            JsonObject compositeComponentsJson = new JsonObject();
            compositeComponentsJson.put(COMPONENTS_LABEL, new JsonArray());
            compositeSegment.getJsonArray(DATA_ELEMENTS_LABEL).add(compositeComponentsJson);
            break;
          case END_COMPOSITE:
            buildingComposite = false;
            break;
          case ELEMENT_DATA:
            String data = reader.getText();
            JsonObject lastSegment = segmentsJson.getJsonObject(segmentsJson.size()-1);
            if(!buildingComposite) {
              JsonObject componentsJson = new JsonObject();
              componentsJson.put(COMPONENTS_LABEL, new JsonArray());
              lastSegment.getJsonArray(DATA_ELEMENTS_LABEL).add(componentsJson);
            }
            JsonObject dataJson = new JsonObject();
            dataJson.put(DATA_LABEL, data);
            JsonArray dataElements = lastSegment.getJsonArray(DATA_ELEMENTS_LABEL);
            JsonObject lastDataElement = dataElements.getJsonObject(dataElements.size()-1);
            lastDataElement.getJsonArray(COMPONENTS_LABEL)
              .add(dataJson);
            break;
          case SEGMENT_ERROR:
            throw new EDIStreamException("Parsing raw EDIFACT record resulted in a SEGMENT_ERROR.");
          case ELEMENT_DATA_ERROR:
            throw new EDIStreamException("Parsing raw EDIFACT record resulted in an ELEMENT_DATA_ERROR.");
          case ELEMENT_OCCURRENCE_ERROR:
            throw new EDIStreamException("Parsing raw EDIFACT record resulted in an ELEMENT_OCCURRENCE_ERROR.");
          }
      }
      reader.close();
      stream.close();
    } catch (EDIStreamException | IOException e) {
      LOGGER.error("Error during parse EDIFACT record from raw record", e);
      prepareResultWithError(result, Collections.singletonList(new JsonObject()
        .put("name", e.getClass().getName())
        .put("message", e.getMessage())));
    }

    result.setParsedRecord(resultJson);
    System.out.println(resultJson);

    return result;
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
