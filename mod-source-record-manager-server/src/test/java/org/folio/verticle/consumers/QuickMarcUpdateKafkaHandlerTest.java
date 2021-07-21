package org.folio.verticle.consumers;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.folio.rest.RestVerticle.OKAPI_HEADER_TENANT;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.SourceRecordState;
import org.folio.services.QuickMarcEventProducerService;
import org.folio.services.SourceRecordStateService;
import org.folio.verticle.consumers.util.QMEventTypes;
import org.folio.verticle.consumers.util.QmCompletedEventPayload;

@RunWith(MockitoJUnitRunner.class)
public class QuickMarcUpdateKafkaHandlerTest {

  private static final String TENANT_ID = "test";

  @Mock
  private KafkaInternalCache kafkaInternalCache;
  @Mock
  private SourceRecordStateService sourceRecordStateService;
  @Mock
  private QuickMarcEventProducerService producerService;
  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;

  @Captor
  private ArgumentCaptor<String> qmCompletedEventCaptor;

  private final Vertx vertx = Vertx.vertx();
  private QuickMarcUpdateKafkaHandler quickMarcHandler;

  @Before
  public void setUp() throws Exception {
    quickMarcHandler = new QuickMarcUpdateKafkaHandler(kafkaInternalCache, sourceRecordStateService, producerService, vertx);
  }

  @Test
  public void shouldNotHandleEventWhenKafkaCacheContainsEventId() throws IOException {
    var recordId = UUID.randomUUID().toString();
    var kafkaHeaders = List.of(KafkaHeader.header(OKAPI_HEADER_TENANT, TENANT_ID));

    Map<String, String> eventPayload = new HashMap<>();
    eventPayload.put("RECORD_ID", recordId);

    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(QMEventTypes.QM_INVENTORY_INSTANCE_UPDATED.name())
      .withEventPayload(ZIPArchiver.zip(Json.encode(eventPayload)));

    String expectedKafkaRecordKey = "1";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));
    when(kafkaInternalCache.containsByKey(contains(event.getId()))).thenReturn(true);

    var future = quickMarcHandler.handle(kafkaRecord);

    verify(kafkaInternalCache, times(1)).containsByKey(contains(event.getId()));
    verify(producerService, never()).sendEvent(anyString(), anyString(), any(), anyString(), anyList());
    verify(sourceRecordStateService, never()).updateState(anyString(), any(SourceRecordState.RecordState.class), anyString());
    assertTrue(future.succeeded());
    assertEquals(expectedKafkaRecordKey, future.result());
  }

  @Test
  public void shouldUpdateRecordStateAndSendEventOnHandleQmInventoryInstanceUpdated() throws IOException {
    var recordId = UUID.randomUUID().toString();
    var kafkaHeaders = List.of(KafkaHeader.header(OKAPI_HEADER_TENANT, TENANT_ID));

    Map<String, String> eventPayload = new HashMap<>();
    eventPayload.put("RECORD_ID", recordId);

    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(QMEventTypes.QM_INVENTORY_INSTANCE_UPDATED.name())
      .withEventPayload(ZIPArchiver.zip(Json.encode(eventPayload)));

    when(kafkaRecord.value()).thenReturn(Json.encode(event));
    when(kafkaRecord.headers()).thenReturn(kafkaHeaders);
    when(kafkaInternalCache.containsByKey(contains(event.getId()))).thenReturn(false);
    when(producerService.sendEvent(anyString(), anyString(), isNull(), anyString(), anyList()))
      .thenReturn(Future.succeededFuture(true));
    when(sourceRecordStateService.updateState(anyString(), any(SourceRecordState.RecordState.class), anyString()))
      .thenReturn(Future.succeededFuture(new SourceRecordState()));

    var future = quickMarcHandler.handle(kafkaRecord);
    assertTrue(future.succeeded());
    verify(producerService, times(1))
      .sendEvent(qmCompletedEventCaptor.capture(), eq(QMEventTypes.QM_COMPLETED.name()), isNull(), eq(TENANT_ID),
        eq(kafkaHeaders));

    verify(sourceRecordStateService, times(1))
      .updateState(recordId, SourceRecordState.RecordState.ACTUAL, TENANT_ID);

    var actualEventPayload = Json.decodeValue(qmCompletedEventCaptor.getValue(), QmCompletedEventPayload.class);
    assertEquals(recordId, actualEventPayload.getRecordId());
    assertNull(actualEventPayload.getErrorMessage());
  }

  @Test
  public void shouldUpdateRecordStateAndSendEventOnHandleQmError() throws IOException {
    var errorMessage = "random error";
    var recordId = UUID.randomUUID().toString();
    var kafkaHeaders = List.of(KafkaHeader.header(OKAPI_HEADER_TENANT, TENANT_ID));

    Map<String, String> eventPayload = new HashMap<>();
    eventPayload.put("RECORD_ID", recordId);
    eventPayload.put("ERROR", errorMessage);

    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(QMEventTypes.QM_ERROR.name())
      .withEventPayload(ZIPArchiver.zip(Json.encode(eventPayload)));

    when(kafkaRecord.value()).thenReturn(Json.encode(event));
    when(kafkaRecord.headers()).thenReturn(kafkaHeaders);
    when(kafkaInternalCache.containsByKey(contains(event.getId()))).thenReturn(false);
    when(producerService.sendEvent(anyString(), anyString(), isNull(), anyString(), anyList()))
      .thenReturn(Future.succeededFuture(true));
    when(sourceRecordStateService.updateState(anyString(), any(SourceRecordState.RecordState.class), anyString()))
      .thenReturn(Future.succeededFuture(new SourceRecordState()));

    var future = quickMarcHandler.handle(kafkaRecord);
    assertTrue(future.succeeded());
    verify(producerService, times(1))
      .sendEvent(qmCompletedEventCaptor.capture(), eq(QMEventTypes.QM_COMPLETED.name()), isNull(), eq(TENANT_ID),
        eq(kafkaHeaders));

    verify(sourceRecordStateService, times(1))
      .updateState(recordId, SourceRecordState.RecordState.ERROR, TENANT_ID);

    var actualEventPayload = Json.decodeValue(qmCompletedEventCaptor.getValue(), QmCompletedEventPayload.class);
    assertEquals(recordId, actualEventPayload.getRecordId());
    assertEquals(errorMessage, actualEventPayload.getErrorMessage());
  }
}