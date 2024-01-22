package org.folio.util;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/**
 * Another representation of {@link org.folio.rest.jaxrs.model.Event} that uses the less
 * expensive DataImportPayload type. This is currently scoped for journal events.
 */
@JsonDeserialize(using = JournalEventDeserializer.class)
public class JournalEvent {
  @JsonProperty("id")
  private String id;

  @JsonProperty("eventType")
  private String eventType;

  @JsonProperty("eventPayload")
  private DataImportEventPayloadWithoutCurrentNode eventPayload;

  public String getId() {
    return id;
  }

  public JournalEvent setId(String id) {
    this.id = id;
    return this;
  }

  public String getEventType() {
    return eventType;
  }

  public JournalEvent setEventType(String eventType) {
    this.eventType = eventType;
    return this;
  }

  public DataImportEventPayloadWithoutCurrentNode getEventPayload() {
    return eventPayload;
  }

  public JournalEvent setEventPayload(DataImportEventPayloadWithoutCurrentNode eventPayload) {
    this.eventPayload = eventPayload;
    return this;
  }
}
