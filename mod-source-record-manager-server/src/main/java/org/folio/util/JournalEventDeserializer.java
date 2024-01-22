package org.folio.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import io.vertx.core.json.jackson.DatabindCodec;

import java.io.CharArrayReader;
import java.io.IOException;

/**
 * Custom deserializer for {@link JournalEvent}. This deserializer understands that the
 * {@link JournalEvent#eventPayload} is a string that has a JSON object embedded within.
 * {@link JournalEvent#eventPayload} is deserialized without creating some intermediate String
 * objects if this custom deserializer was not used.
 * <p>
 *   This was needed because the event payload is very large, containing "current node" information.
 *   Multiple string representations of the event payload costed more memory than needed.
 * </p>
 */
public class JournalEventDeserializer extends JsonDeserializer<JournalEvent> {

  @Override
  public JournalEvent deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
    throws IOException {
    JournalEvent journalEvent = new JournalEvent();

    while (!jsonParser.isClosed()) {
      JsonToken jsonToken = jsonParser.nextToken();

      if (JsonToken.FIELD_NAME.equals(jsonToken)) {
        String fieldName = jsonParser.getCurrentName();
        jsonParser.nextToken(); // move to the value of the field
        switch (fieldName) {
          case "id":
            journalEvent.setId(jsonParser.getValueAsString());
            break;
          case "eventType":
            journalEvent.setEventType(jsonParser.getValueAsString());
            break;
          case "eventPayload":
            // get a char array which is already created within the JsonParser for the purpose of decoding to text
            // and not creating a string representation
            try (CharArrayReader charArrayReader = new CharArrayReader(jsonParser.getTextCharacters())) {
              DataImportEventPayloadWithoutCurrentNode payload = DatabindCodec.mapper().readValue(charArrayReader, DataImportEventPayloadWithoutCurrentNode.class);
              journalEvent.setEventPayload(payload);
            }
            break;
        }
      }
    }

    return journalEvent;
  }
}
