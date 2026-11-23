package org.folio.verticle.consumers.errorhandlers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.verticle.consumers.errorhandlers.DiEventRecordBuilder.EVENT_ID;
import static org.folio.verticle.consumers.errorhandlers.DiEventRecordBuilder.aDiEventRecord;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.sql.SQLException;
import java.util.List;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.rest.jaxrs.model.Event;
import org.folio.services.exceptions.DeduplicationLookupException;
import org.folio.services.util.EventHandlingUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class DataImportErrorHandlerTest {

  private final KafkaConfig kafkaConfig = KafkaConfig.builder().build();
  private final Vertx vertx = mock(Vertx.class);
  private final DataImportErrorHandler handler = new DataImportErrorHandler(vertx, kafkaConfig, new RedeliveryPolicy());

  @Before
  public void runTimersImmediately() {
    when(vertx.setTimer(anyLong(), any())).thenAnswer(invocation -> {
      invocation.<Handler<Long>>getArgument(1).handle(1L);
      return 1L;
    });
  }

  @Test
  public void shouldRepublishOriginalEvent_whenDeduplicationLookupFailed() {
    // given
    var record = aDiEventRecord().withHeader("recordId", "r1").build();
    try (MockedStatic<EventHandlingUtil> util = Mockito.mockStatic(EventHandlingUtil.class)) {
      util.when(() -> EventHandlingUtil.sendEventToKafka(any(Event.class), any(List.class), any(KafkaConfig.class), any()))
        .thenReturn(Future.succeededFuture(true));
      var eventCaptor = ArgumentCaptor.forClass(Event.class);
      var headersCaptor = ArgumentCaptor.<List<KafkaHeader>>captor();

      // when
      handler.handle(new DeduplicationLookupException(new SQLException("timeout")), record);

      // then
      util.verify(() -> EventHandlingUtil.sendEventToKafka(eventCaptor.capture(), headersCaptor.capture(),
        any(KafkaConfig.class), any()));
      assertThat(eventCaptor.getValue().getId()).isEqualTo(EVENT_ID);
      assertThat(headersCaptor.getValue())
        .anyMatch(header -> RedeliveryPolicy.RETRY_COUNT_HEADER.equals(header.key()) && "1".equals(header.value().toString()));
    }
  }

  @Test
  public void shouldScheduleRepublishWithBackoff_whenDeduplicationLookupFailed() {
    // given
    var record = aDiEventRecord().withHeader(RedeliveryPolicy.RETRY_COUNT_HEADER, "2").build();
    try (MockedStatic<EventHandlingUtil> util = Mockito.mockStatic(EventHandlingUtil.class)) {
      util.when(() -> EventHandlingUtil.sendEventToKafka(any(Event.class), any(List.class), any(KafkaConfig.class), any()))
        .thenReturn(Future.succeededFuture(true));

      // when
      handler.handle(new DeduplicationLookupException(new SQLException("timeout")), record);

      // then
      verify(vertx).setTimer(eq(4000L), any());
    }
  }

  @Test
  public void shouldNotRepublishEvent_whenRetriesExhausted() {
    // given
    var record = aDiEventRecord()
      .withHeader(RedeliveryPolicy.RETRY_COUNT_HEADER, String.valueOf(RedeliveryPolicy.MAX_RETRIES))
      .build();
    try (MockedStatic<EventHandlingUtil> util = Mockito.mockStatic(EventHandlingUtil.class)) {

      // when
      handler.handle(new DeduplicationLookupException(new SQLException("timeout")), record);

      // then
      verify(vertx, never()).setTimer(anyLong(), any());
      util.verifyNoInteractions();
    }
  }

  @Test
  public void shouldNotRepublishEvent_whenFailureIsNotDeduplicationLookup() {
    // given
    var record = aDiEventRecord().build();
    try (MockedStatic<EventHandlingUtil> util = Mockito.mockStatic(EventHandlingUtil.class)) {

      // when
      handler.handle(new DuplicateEventException("duplicate"), record);

      // then
      verify(vertx, never()).setTimer(anyLong(), any());
      util.verifyNoInteractions();
    }
  }
}
