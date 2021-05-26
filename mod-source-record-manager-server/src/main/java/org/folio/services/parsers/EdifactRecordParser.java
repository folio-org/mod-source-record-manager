package org.folio.services.parsers;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.folio.rest.jaxrs.model.Component;
import org.folio.rest.jaxrs.model.DataElement;
import org.folio.rest.jaxrs.model.EdifactParsedContent;
import org.folio.rest.jaxrs.model.RecordsMetadata;
import org.folio.rest.jaxrs.model.Segment;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.xlate.edi.stream.EDIInputFactory;
import io.xlate.edi.stream.EDIStreamReader;

import static io.xlate.edi.stream.EDIInputFactory.EDI_VALIDATE_CONTROL_CODE_VALUES;

/**
 * Raw record parser implementation for EDIFACT format. Use staedi library
 */
public final class EdifactRecordParser implements RecordParser {

  private static final Logger LOGGER = LogManager.getLogger();

  @Override
  public ParsedResult parseRecord(String rawRecord) {
    EDIInputFactory ediInputFactory = EDIInputFactory.newFactory();
    ediInputFactory.setProperty(EDI_VALIDATE_CONTROL_CODE_VALUES, false);
    ParsedResult result = new ParsedResult();

    List<Segment> segments = new ArrayList<>();

    List<JsonObject> errorList = new ArrayList<>();
    boolean buildingComposite = false;

    try (
      InputStream stream = new ByteArrayInputStream(rawRecord.getBytes());
      EDIStreamReader reader = ediInputFactory.createEDIStreamReader(stream);
    ) {
      while (reader.hasNext()) {
        switch (reader.next()) {
          case START_INTERCHANGE:
            break;
          case START_SEGMENT:
            Segment segment = new Segment()
              .withTag(reader.getText())
              .withDataElements(new ArrayList<DataElement>());
            segments.add(segment);
            break;
          case END_SEGMENT:
            break;
          case START_COMPOSITE:
            buildingComposite = true;
            segments.get(segments.size() - 1)
              .getDataElements()
              .add(new DataElement());
            break;
          case END_COMPOSITE:
            buildingComposite = false;
            break;
          case ELEMENT_DATA:
            Segment lastSegment = segments.get(segments.size() - 1);
            if(!buildingComposite) {
              lastSegment
                .getDataElements()
                .add(new DataElement());
            }
            List<DataElement> dataElements = lastSegment.getDataElements();
            dataElements.get(dataElements.size() - 1)
              .getComponents()
              .add(new Component()
                .withData(reader.getText()));
            break;
          case ELEMENT_DATA_ERROR:
            errorList.add(processParsingEventError(reader));
            break;
          case SEGMENT_ERROR:
          case ELEMENT_OCCURRENCE_ERROR:
            errorList.add(processParsingEventError(reader));
            break;
          default:
            // ELEMENT_DATA_BINARY, START_GROUP, END_GROUP, START_LOOP, END_LOOP, START_TRANSACTION, END_TRANSACTION, END_INTERCHANGE
            break;
          }
      }
    } catch (Exception e) {
      LOGGER.error("Error during parse EDIFACT record from raw record", e);
      prepareResultWithError(result, Collections.singletonList(new JsonObject()
        .put("name", e.getClass().getName())
        .put("message", e.getMessage())));
    }

    if (!errorList.isEmpty()) {
      prepareResultWithError(result, errorList);
    }

    String encodedResult = Json.encode(new EdifactParsedContent()
      .withSegments(segments));
    JsonObject parsedResult = new JsonObject(encodedResult);
    result.setParsedRecord(parsedResult);
    return result;
  }

  private JsonObject processParsingEventError(EDIStreamReader reader) {
    LOGGER.error("Error during parse EDIFACT {} {}, from the {} event.", reader.getText(), reader.getErrorType(), reader.getEventType());
    return buildErrorObject(reader.getText(), reader.getErrorType().name());
  }

  /**
   * Build json representation of EDIFACT error
   *
   * @param tag - event tag
   * @param message - error message
   * @return - JsonObject with error descriptions
   */
  private JsonObject buildErrorObject(String tag, String message) {
    JsonObject errorJson = new JsonObject();
    errorJson.put("tag", tag);
    errorJson.put("message", message);
    return errorJson;
  }

  @Override
  public RecordsMetadata.ContentType getParserFormat() {
    return RecordsMetadata.ContentType.EDIFACT_RAW;
  }
}
