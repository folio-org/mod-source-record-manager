package org.folio.verticle.consumers.errorhandlers;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.Event;
import org.folio.services.exceptions.DeduplicationLookupException;
import org.folio.services.util.EventHandlingUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class DataImportErrorHandlerTest {

  private final KafkaConfig kafkaConfig = KafkaConfig.builder().build();
  private final Vertx vertx = Mockito.mock(Vertx.class);
  private final DataImportErrorHandler handler = new DataImportErrorHandler(vertx, kafkaConfig);

  @Before
  public void runTimersImmediately() {
    Mockito.when(vertx.setTimer(Mockito.anyLong(), any())).thenAnswer(inv -> {
      inv.<Handler<Long>>getArgument(1).handle(1L);
      return 1L;
    });
  }

  @Test
  public void shouldRepublishEventWhenDeduplicationLookupFailed() {
    try (MockedStatic<EventHandlingUtil> util = Mockito.mockStatic(EventHandlingUtil.class)) {
      util.when(
          () -> EventHandlingUtil.sendEventToKafka(any(Event.class), any(List.class), any(KafkaConfig.class), any()))
        .thenReturn(Future.succeededFuture(true));
      handler.handle(new DeduplicationLookupException("timeout", new SQLException()), record(null));

      util.verify(() -> EventHandlingUtil.sendEventToKafka(
        argThat((Event e) -> "c9d09a5e-73ba-11ec-90d6-0242ac120003".equals(e.getId())),
        any(List.class), eq(kafkaConfig), any()), times(1));
    }
  }

  @Test
  public void shouldNotRepublishEventWhenRetriesExhausted() {
    try (MockedStatic<EventHandlingUtil> util = Mockito.mockStatic(EventHandlingUtil.class)) {
      handler.handle(new DeduplicationLookupException("timeout", new SQLException()),
        record(String.valueOf(DataImportErrorHandler.MAX_RETRIES)));

      util.verify(() -> EventHandlingUtil.sendEventToKafka(any(Event.class), any(List.class),
        any(KafkaConfig.class), any()), never());
    }
  }

  @Test
  public void shouldNotRepublishEventWhenFailureIsNotDeduplicationLookup() {
    try (MockedStatic<EventHandlingUtil> util = Mockito.mockStatic(EventHandlingUtil.class)) {
      handler.handle(new DuplicateEventException("duplicate"), record(null));

      util.verify(() -> EventHandlingUtil.sendEventToKafka(any(Event.class), any(List.class),
        any(KafkaConfig.class), any()), never());
    }
  }

  private KafkaConsumerRecordImpl<String, byte[]> record(String retryCount) {
    Event event = new Event().withId("c9d09a5e-73ba-11ec-90d6-0242ac120003")
      .withEventType(DI_ERROR.value()).withEventPayload("{}")
      .withEventMetadata(new org.folio.rest.jaxrs.model.EventMetadata().withTenantId("diku"));
    ConsumerRecord<String, byte[]> consumerRecord =
      new ConsumerRecord<>("topic", 0, 0, "1", Json.encode(event).getBytes(StandardCharsets.UTF_8));
    consumerRecord.headers().add(new RecordHeader(XOkapiHeaders.TENANT, "diku".getBytes()));
    consumerRecord.headers().add(new RecordHeader("recordId", "r1".getBytes()));
    if (retryCount != null) {
      consumerRecord.headers().add(new RecordHeader(DataImportErrorHandler.RETRY_COUNT_HEADER, retryCount.getBytes()));
    }
    return new KafkaConsumerRecordImpl<>(consumerRecord);
  }
}
