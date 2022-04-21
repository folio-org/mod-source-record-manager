package org.folio.verticle.consumers.errorhandlers;

import static org.mockito.ArgumentMatchers.any;

import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.Event;
import org.folio.services.util.EventHandlingUtil;

class RawMarcChunksErrorHandlerTest {

  private final RawMarcChunksErrorHandler errorHandler = new RawMarcChunksErrorHandler();

  @Test
  void shouldPopulateEventPayloadWithHeadersData() {

    KafkaConsumerRecord<String, String> record = new KafkaConsumerRecordImpl<>(new ConsumerRecord<>("topic",
      0, 0, "key", Json.encode(new Event())));

    try (var mockedStatic = Mockito.mockStatic(EventHandlingUtil.class)) {
      errorHandler.handle(new Throwable(), record);

      mockedStatic.verify(() -> EventHandlingUtil.populatePayloadWithHeadersData(any(DataImportEventPayload.class), any()));
    }
  }

}
