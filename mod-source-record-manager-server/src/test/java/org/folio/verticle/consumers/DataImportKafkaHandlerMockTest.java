package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.rest.jaxrs.model.Event;
import org.folio.services.EventHandlingService;
import org.folio.services.EventProcessedService;
import org.folio.services.flowcontrol.RawRecordsFlowControlService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.UUID;

import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.services.mappers.processor.MappingParametersProviderTest.SYSTEM_USER_ENABLED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class DataImportKafkaHandlerMockTest {

  private static final String TENANT_ID = "diku";
  private static final String TOKEN = "token";
  private static final String KAFKA_ENV = "folio";
  private static final String DI_KAFKA_HANDLER_ID = "6713adda-72ce-11ec-90d6-0242ac120003";

  @Spy
  private Vertx vertx = Vertx.vertx();
  @Mock
  private EventHandlingService eventHandlingService;
  @Mock
  private EventProcessedService eventProcessedService;
  @Mock
  private RawRecordsFlowControlService flowControlService;
  private DataImportKafkaHandler dataImportKafkaHandler;

  @Before
  public void setUp() {
    dataImportKafkaHandler = new DataImportKafkaHandler(vertx, eventHandlingService, eventProcessedService, flowControlService);
  }

  @Test
  public void shouldSkipEventHandlingWhenDBContainsHandlerAndEventId() {
    // given
    Mockito.when(eventProcessedService.collectData(eq(DI_KAFKA_HANDLER_ID), eq("c9d09a5e-73ba-11ec-90d6-0242ac120003"), eq(TENANT_ID)))
      .thenReturn(Future.failedFuture(new DuplicateEventException("Constraint Violation Occurs")));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(new HashMap<>());

    Event event = new Event().withEventType(DI_ERROR.value())
      .withId("c9d09a5e-73ba-11ec-90d6-0242ac120003")
      .withEventPayload(Json.encode(dataImportEventPayload));
    String topic = KafkaTopicNameHelper.formatTopicName(KAFKA_ENV, getDefaultNameSpace(), TENANT_ID, event.getEventType());
    ConsumerRecord<String, byte[]> consumerRecord = new ConsumerRecord<>(topic, 0, 0, "1", Json.encode(event).getBytes(StandardCharsets.UTF_8));
    consumerRecord.headers().add(new RecordHeader(OKAPI_TENANT_HEADER, TENANT_ID.getBytes()));
    consumerRecord.headers().add(new RecordHeader("recordId", UUID.randomUUID().toString().getBytes()));
    KafkaConsumerRecordImpl<String, byte[]> kafkaRecord = new KafkaConsumerRecordImpl<>(consumerRecord);

    // when
    Future<String> future = dataImportKafkaHandler.handle(kafkaRecord);

    // then
    Assert.assertTrue(future.succeeded());
    Assert.assertTrue(future.isComplete());
    verify(eventHandlingService, never()).handle(anyString(), any(OkapiConnectionParams.class));
  }

  @Test
  public void shouldHandleWhenSystemUserEnabled() {
    // given
    System.setProperty(SYSTEM_USER_ENABLED, "false");
    // given
    Mockito.when(eventHandlingService.handle(anyString(), any(OkapiConnectionParams.class))).thenReturn(Future.succeededFuture());
    Mockito.when(eventProcessedService.collectData(eq(DI_KAFKA_HANDLER_ID),eq("c9d09a5e-73ba-11ec-90d6-0242ac120003"), eq(TENANT_ID)))
      .thenReturn(Future.succeededFuture());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(new HashMap<>());

    Event event = new Event()
      .withId("c9d09a5e-73ba-11ec-90d6-0242ac120003")
      .withEventType(DI_ERROR.value())
      .withEventPayload(Json.encode(dataImportEventPayload));
    String topic = KafkaTopicNameHelper.formatTopicName(KAFKA_ENV, getDefaultNameSpace(), TENANT_ID, event.getEventType());
    ConsumerRecord<String, byte[]> consumerRecord = new ConsumerRecord<>(topic, 0, 0, "1", Json.encode(event).getBytes(StandardCharsets.UTF_8));
    consumerRecord.headers().add(new RecordHeader(OKAPI_TENANT_HEADER, TENANT_ID.getBytes()));
    consumerRecord.headers().add(new RecordHeader(OKAPI_TOKEN_HEADER, TOKEN.getBytes()));
    consumerRecord.headers().add(new RecordHeader("recordId", UUID.randomUUID().toString().getBytes()));
    KafkaConsumerRecordImpl<String, byte[]> kafkaRecord = new KafkaConsumerRecordImpl<>(consumerRecord);

    Future<String> future = dataImportKafkaHandler.handle(kafkaRecord);
    System.clearProperty(SYSTEM_USER_ENABLED);

    // then
    Assert.assertTrue(future.succeeded());
    Assert.assertTrue(future.isComplete());

    verify(eventHandlingService, times(1)).handle(anyString(), argThat(params -> StringUtils.isEmpty(params.getToken())));
  }

  @Test
  public void shouldHandleWhenThereIsNoRecordIdInTheKafkaRecord() {
    // given
    Mockito.when(eventHandlingService.handle(anyString(), any(OkapiConnectionParams.class))).thenReturn(Future.succeededFuture());
    Mockito.when(eventProcessedService.collectData(eq(DI_KAFKA_HANDLER_ID),eq("c9d09a5e-73ba-11ec-90d6-0242ac120003"), eq(TENANT_ID)))
      .thenReturn(Future.succeededFuture());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(new HashMap<>());

    Event event = new Event()
      .withId("c9d09a5e-73ba-11ec-90d6-0242ac120003")
      .withEventType(DI_ERROR.value())
      .withEventPayload(Json.encode(dataImportEventPayload));
    String topic = KafkaTopicNameHelper.formatTopicName(KAFKA_ENV, getDefaultNameSpace(), TENANT_ID, event.getEventType());
    ConsumerRecord<String, byte[]> consumerRecord = new ConsumerRecord<>(topic, 0, 0, "1", Json.encode(event).getBytes(StandardCharsets.UTF_8));
    consumerRecord.headers().add(new RecordHeader(OKAPI_TENANT_HEADER, TENANT_ID.getBytes()));
    consumerRecord.headers().add(new RecordHeader(OKAPI_TOKEN_HEADER, TOKEN.getBytes()));
    consumerRecord.headers().add(new RecordHeader("recordId", UUID.randomUUID().toString().getBytes()));
    KafkaConsumerRecordImpl<String, byte[]> kafkaRecord = new KafkaConsumerRecordImpl<>(consumerRecord);

    Future<String> future = dataImportKafkaHandler.handle(kafkaRecord);

    // then
    Assert.assertTrue(future.succeeded());
    Assert.assertTrue(future.isComplete());

    verify(eventHandlingService, times(1)).handle(anyString(), argThat(params -> StringUtils.isNotEmpty(params.getToken())));
  }

  @Test
  public void shouldFailWhenThereAnErrorWithDBConnection() {
    // given
    Mockito.when(eventProcessedService.collectData(eq(DI_KAFKA_HANDLER_ID), eq("c9d09a5e-73ba-11ec-90d6-0242ac120003"), eq(TENANT_ID)))
      .thenReturn(Future.failedFuture(new SQLException("Connection timeout!")));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(new HashMap<>());

    Event event = new Event()
      .withId("c9d09a5e-73ba-11ec-90d6-0242ac120003")
      .withEventType(DI_ERROR.value())
      .withEventPayload(Json.encode(dataImportEventPayload));
    String topic = KafkaTopicNameHelper.formatTopicName(KAFKA_ENV, getDefaultNameSpace(), TENANT_ID, event.getEventType());
    ConsumerRecord<String, byte[]> consumerRecord = new ConsumerRecord<>(topic, 0, 0, "1", Json.encode(event).getBytes(StandardCharsets.UTF_8));
    consumerRecord.headers().add(new RecordHeader(OKAPI_TENANT_HEADER, TENANT_ID.getBytes()));
    consumerRecord.headers().add(new RecordHeader("recordId", UUID.randomUUID().toString().getBytes()));
    KafkaConsumerRecordImpl<String, byte[]> kafkaRecord = new KafkaConsumerRecordImpl<>(consumerRecord);

    Future<String> future = dataImportKafkaHandler.handle(kafkaRecord);

    // then
    Assert.assertTrue(future.failed());
    Assert.assertTrue(future.cause() instanceof SQLException);
    Assert.assertTrue(future.isComplete());
    verify(eventHandlingService, never()).handle(anyString(), any(OkapiConnectionParams.class));
  }
}
