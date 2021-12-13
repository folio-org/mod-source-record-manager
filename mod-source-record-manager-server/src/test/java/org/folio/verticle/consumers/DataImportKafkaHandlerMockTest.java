package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.rest.jaxrs.model.Event;
import org.folio.services.EventHandlingService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.UUID;

import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.verticle.consumers.DataImportKafkaHandler.RECORD_ID_HEADER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class DataImportKafkaHandlerMockTest {

  private static final String TENANT_ID = "diku";
  private static final String KAFKA_ENV = "folio";

  @Spy
  private Vertx vertx = Vertx.vertx();
  @Mock
  private EventHandlingService eventHandlingService;
  @Mock
  private KafkaInternalCache kafkaInternalCache;
  @InjectMocks
  private DataImportKafkaHandler dataImportKafkaHandler;

  @Test
  public void shouldSkipEventHandlingWhenKafkaCacheContainsRecordId() {
    // given
    Mockito.when(kafkaInternalCache.containsByKey(anyString())).thenReturn(true);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(new HashMap<>());

    Event event = new Event().withEventType(DI_ERROR.value()).withEventPayload(Json.encode(dataImportEventPayload));
    KafkaConsumerRecordImpl<String, String> kafkaRecord = buildKafkaRecord(event);

    // when
    Future<String> future = dataImportKafkaHandler.handle(kafkaRecord);

    // then
    Assert.assertTrue(future.succeeded());
    verify(eventHandlingService, never()).handle(anyString(), any(OkapiConnectionParams.class));
  }

  @Test
  public void shouldHandleWhenThereIsNoRecordIdInTheKafkaRecord() {
    // given
    Mockito.when(eventHandlingService.handle(anyString(), any(OkapiConnectionParams.class))).thenReturn(Future.succeededFuture());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(new HashMap<>());

    Event event = new Event().withEventType(DI_ERROR.value()).withEventPayload(Json.encode(dataImportEventPayload));

    String topic = KafkaTopicNameHelper.formatTopicName(KAFKA_ENV, getDefaultNameSpace(), TENANT_ID, event.getEventType());
    ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>(topic, 0, 0, "1", Json.encode(event));
    consumerRecord.headers().add(new RecordHeader(OKAPI_TENANT_HEADER, TENANT_ID.getBytes()));
    KafkaConsumerRecordImpl<String, String> kafkaRecord = new KafkaConsumerRecordImpl<>(consumerRecord);

    Future<String> future = dataImportKafkaHandler.handle(kafkaRecord);

    // then
    Assert.assertTrue(future.succeeded());
    verify(eventHandlingService, times(1)).handle(anyString(), any(OkapiConnectionParams.class));
  }

  private KafkaConsumerRecordImpl<String, String> buildKafkaRecord(Event event) {
    String topic = KafkaTopicNameHelper.formatTopicName(KAFKA_ENV, getDefaultNameSpace(), TENANT_ID, event.getEventType());
    ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>(topic, 0, 0, "1", Json.encode(event));
    consumerRecord.headers().add(new RecordHeader(OKAPI_TENANT_HEADER, TENANT_ID.getBytes()));
    consumerRecord.headers().add(new RecordHeader(RECORD_ID_HEADER, UUID.randomUUID().toString().getBytes()));
    return new KafkaConsumerRecordImpl<>(consumerRecord);
  }
}
