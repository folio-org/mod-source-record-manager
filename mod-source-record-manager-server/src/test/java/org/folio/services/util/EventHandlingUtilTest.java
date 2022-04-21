package org.folio.services.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import static org.folio.services.util.EventHandlingUtil.CORRELATION_ID_HEADER;

import java.util.Map;
import java.util.UUID;

import io.vertx.core.Vertx;
import org.junit.jupiter.api.Test;

import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.DataImportEventPayload;

class EventHandlingUtilTest {

  @Test
  void shouldPopulateEventPayloadWithCorrelationId() {
    String correlationId = UUID.randomUUID().toString();
    DataImportEventPayload eventPayload = new DataImportEventPayload();
    OkapiConnectionParams params = new OkapiConnectionParams(Map.of(CORRELATION_ID_HEADER, correlationId), mock(Vertx.class));

    EventHandlingUtil.populatePayloadWithHeadersData(eventPayload, params);

    assertEquals(correlationId, eventPayload.getCorrelationId());
  }

  @Test
  void shouldPopulateFolioEventPayloadWithCorrelationId() {
    String correlationId = UUID.randomUUID().toString();
    org.folio.DataImportEventPayload eventPayload = new org.folio.DataImportEventPayload();
    OkapiConnectionParams params = new OkapiConnectionParams(Map.of(CORRELATION_ID_HEADER, correlationId), mock(Vertx.class));

    EventHandlingUtil.populatePayloadWithHeadersData(eventPayload, params);

    assertEquals(correlationId, eventPayload.getCorrelationId());
  }

}
