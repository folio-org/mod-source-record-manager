package org.folio.verticle.consumers.errorhandlers;

import static org.mockito.ArgumentMatchers.any;

import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;

import java.util.Collections;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.services.util.EventHandlingUtil;

class StoredRecordChunksErrorHandlerTest {

  private final StoredRecordChunksErrorHandler errorHandler = new StoredRecordChunksErrorHandler(null, null);

  @Test
  void shouldPopulateEventPayloadWithHeadersData() {
    Record record = new Record().withRecordType(MARC_BIB);
    Event event = new Event().withEventPayload(Json.encode(new RecordsBatchResponse()
      .withRecords(Collections.singletonList(record))));
    KafkaConsumerRecord<String, String> kafkaRecord = new KafkaConsumerRecordImpl<>(new ConsumerRecord<>("topic",
      0, 0, "key", Json.encode(event)));

    try (var mockedStatic = Mockito.mockStatic(EventHandlingUtil.class)) {
      mockedStatic.when(() -> EventHandlingUtil.sendEventToKafka(any(), any(), any(), any(), any(), any()))
        .thenReturn(Future.succeededFuture(true));

      errorHandler.handle(new Throwable(), kafkaRecord);

      mockedStatic.verify(() -> EventHandlingUtil.populatePayloadWithHeadersData(any(DataImportEventPayload.class), any()));
    }
  }

}
