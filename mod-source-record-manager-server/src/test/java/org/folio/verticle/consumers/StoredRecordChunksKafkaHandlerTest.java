package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.commons.lang.StringUtils;
import org.folio.TestUtil;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JournalRecord.EntityType;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.services.EventProcessedService;
import org.folio.services.JobExecutionService;
import org.folio.services.MappingRuleCache;
import org.folio.services.RecordsPublishingService;
import org.folio.services.entity.MappingRuleCacheKey;
import org.folio.services.journal.BatchableJournalRecord;
import org.folio.services.journal.JournalUtil;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.folio.rest.RestVerticle.OKAPI_HEADER_TENANT;
import static org.folio.rest.RestVerticle.OKAPI_HEADER_TOKEN;
import static org.folio.services.mappers.processor.MappingParametersProviderTest.SYSTEM_USER_ENABLED;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.folio.verticle.consumers.StoredRecordChunksKafkaHandler.STORED_RECORD_CHUNKS_KAFKA_HANDLER_UUID;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class StoredRecordChunksKafkaHandlerTest {

  private static final String MARC_BIB_RECORD_PATH = "src/test/resources/org/folio/rest/record.json";
  private static final String MARC_AUTHORITY_RECORD_PATH = "src/test/resources/org/folio/rest/marcAuthorityRecord.json";
  private static final String MARC_HOLDING_RECORD_PATH = "src/test/resources/org/folio/rest/marcHoldingRecord.json";
  private static final String EDIFACT_RECORD_PATH = "src/test/resources/org/folio/rest/edifactRecord.json";
  private static final String MAPPING_RULES_PATH = "src/test/resources/org/folio/services/marc_bib_rules.json";
  private static final String TENANT_ID = "diku";
  private static final String TOKEN = "token";

  private static JsonObject mappingRules;

  @Mock
  private RecordsPublishingService recordsPublishingService;
  @Mock
  private KafkaConsumerRecord<String, byte[]> kafkaRecord;
  @Mock
  private EventProcessedService eventProcessedService;
  @Mock
  private MappingRuleCache mappingRuleCache;
  @Mock
  private JobExecutionService jobExecutionService;
  @Mock
  private MessageProducer<Collection<BatchableJournalRecord>> messageProducer;

  private Vertx vertx = JournalUtil.registerCodecs(Vertx.vertx());
  private AsyncRecordHandler<String, byte[]> storedRecordChunksKafkaHandler;

  @BeforeClass
  public static void setUpClass() throws IOException {
    mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
  }

  @Before
  public void setUp() {
    storedRecordChunksKafkaHandler = new StoredRecordChunksKafkaHandler(recordsPublishingService, eventProcessedService,jobExecutionService, mappingRuleCache,  vertx);
    when(jobExecutionService.getJobExecutionById(anyString(), anyString()))
      .thenReturn(Future.succeededFuture(Optional.of(new JobExecution())));
    ReflectionTestUtils.setField(storedRecordChunksKafkaHandler, "journalRecordProducer", messageProducer);
  }

  @Test
  public void shouldNotHandleEventWhenDuplicateComing() {
    // given
    RecordsBatchResponse recordsBatch = new RecordsBatchResponse()
      .withRecords(List.of(new Record()))
      .withTotalRecords(1);

    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(Json.encode(recordsBatch));

    when(kafkaRecord.value()).thenReturn(Json.encode(event).getBytes(StandardCharsets.UTF_8));
    when(kafkaRecord.headers()).thenReturn(List.of(KafkaHeader.header(OKAPI_HEADER_TENANT.toLowerCase(), TENANT_ID)));
    when(eventProcessedService.collectData(STORED_RECORD_CHUNKS_KAFKA_HANDLER_UUID, event.getId(), TENANT_ID))
      .thenReturn(Future.failedFuture(new DuplicateEventException("Constraint Violation Occurs")));
    when(jobExecutionService.getJobExecutionById(any(), any())).thenReturn(Future.succeededFuture(Optional.of(new JobExecution())));

    // when
    Future<String> future = storedRecordChunksKafkaHandler.handle(kafkaRecord);

    // then
    verify(recordsPublishingService, never()).sendEventsWithRecords(anyList(), anyString(), any(OkapiConnectionParams.class), anyString(), any());
    assertTrue(future.failed());
    assertTrue(future.cause() instanceof DuplicateEventException);
  }

  @Test
  public void shouldWriteSavedMarcBibRecordsInfoToImportJournal() throws IOException {
    writeSavedRecordsInfoToImportJournal(MARC_BIB_RECORD_PATH, EntityType.MARC_BIBLIOGRAPHIC);
  }

  @Test
  public void shouldWriteSavedMarcHoldingRecordsInfoToImportJournal() throws IOException {
    writeSavedRecordsInfoToImportJournal(MARC_HOLDING_RECORD_PATH, EntityType.MARC_HOLDINGS);
  }

  @Test
  public void shouldWriteSavedMarcAuthorityRecordsInfoToImportJournal() throws IOException {
    writeSavedRecordsInfoToImportJournal(MARC_AUTHORITY_RECORD_PATH, EntityType.MARC_AUTHORITY);
  }

  @Test
  public void shouldWriteSavedEdifactRecordsInfoToImportJournal() throws IOException {
    writeSavedRecordsInfoToImportJournal(EDIFACT_RECORD_PATH, EntityType.EDIFACT);
  }

  @Test
  public void writeSavedRecordsInfoToImportJournalWhenSystemUserEnabled()
    throws IOException {
    // given
    System.setProperty(SYSTEM_USER_ENABLED, "false");
    Record record = Json.decodeValue(TestUtil.readFileFromPath(MARC_BIB_RECORD_PATH), Record.class);

    RecordsBatchResponse savedRecordsBatch = new RecordsBatchResponse()
      .withRecords(List.of(record))
      .withTotalRecords(1);

    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(Json.encode(savedRecordsBatch));

    when(kafkaRecord.value()).thenReturn(Json.encode(event).getBytes(StandardCharsets.UTF_8));
    when(kafkaRecord.headers()).thenReturn(List.of(KafkaHeader.header(OKAPI_HEADER_TENANT.toLowerCase(), TENANT_ID),
      KafkaHeader.header(OKAPI_HEADER_TOKEN.toLowerCase(), TOKEN),
      KafkaHeader.header("jobExecutionId", UUID.randomUUID().toString())));
    when(eventProcessedService.collectData(STORED_RECORD_CHUNKS_KAFKA_HANDLER_UUID, event.getId(), TENANT_ID)).thenReturn(Future.succeededFuture());
    when(mappingRuleCache.get(new MappingRuleCacheKey(TENANT_ID, EntityType.MARC_BIBLIOGRAPHIC))).thenReturn(Future.succeededFuture(Optional.of(mappingRules)));

    when(recordsPublishingService
      .sendEventsWithRecords(anyList(), anyString(), any(OkapiConnectionParams.class), anyString(), any()))
      .thenReturn(Future.succeededFuture(true));

    // when
    Future<String> future = storedRecordChunksKafkaHandler.handle(kafkaRecord);
    System.clearProperty(SYSTEM_USER_ENABLED);

    // then
    assertTrue(future.succeeded());
    verify(messageProducer, times(1)).write(any());
    verify(recordsPublishingService).sendEventsWithRecords(anyList(), anyString(), argThat(params -> StringUtils.isEmpty(params.getToken())), anyString(), any());
  }

  @Test
  public void shouldWriteSavedMarcAuthorityRecordsInfoToImportJournalWhenTitleFieldTagIsNull() throws IOException {
    // given
    Record record = Json.decodeValue(TestUtil.readFileFromPath(MARC_AUTHORITY_RECORD_PATH), Record.class);
    JsonObject mappingRulesCopy = mappingRules.copy();
    mappingRulesCopy.remove("245");

    RecordsBatchResponse savedRecordsBatch = new RecordsBatchResponse()
      .withRecords(List.of(record))
      .withTotalRecords(1);

    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(Json.encode(savedRecordsBatch));

    when(kafkaRecord.value()).thenReturn(Json.encode(event).getBytes(StandardCharsets.UTF_8));
    when(kafkaRecord.headers()).thenReturn(List.of(KafkaHeader.header(OKAPI_HEADER_TENANT.toLowerCase(), TENANT_ID), KafkaHeader.header("jobExecutionId", UUID.randomUUID().toString())));
    when(eventProcessedService.collectData(STORED_RECORD_CHUNKS_KAFKA_HANDLER_UUID, event.getId(), TENANT_ID)).thenReturn(Future.succeededFuture());
    when(mappingRuleCache.get(new MappingRuleCacheKey(TENANT_ID, EntityType.MARC_AUTHORITY))).thenReturn(Future.succeededFuture(Optional.of(mappingRulesCopy)));
    when(recordsPublishingService
      .sendEventsWithRecords(anyList(), anyString(), any(OkapiConnectionParams.class), anyString(), any()))
      .thenReturn(Future.succeededFuture(true));

    // when
    Future<String> future = storedRecordChunksKafkaHandler.handle(kafkaRecord);

    // then
    assertTrue(future.succeeded());
    verify(messageProducer, times(1)).write(any());
  }

  @Test
  public void shouldReturnFailedWhenRecordsIsEmpty() {
    RecordsBatchResponse savedRecordsBatch = new RecordsBatchResponse()
      .withRecords(List.of())
      .withTotalRecords(1);

    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(Json.encode(savedRecordsBatch));

    when(kafkaRecord.value()).thenReturn(Json.encode(event).getBytes(StandardCharsets.UTF_8));
    when(kafkaRecord.headers()).thenReturn(List.of(KafkaHeader.header(OKAPI_HEADER_TENANT.toLowerCase(), TENANT_ID), KafkaHeader.header("jobExecutionId", UUID.randomUUID().toString())));
    when(eventProcessedService.collectData(STORED_RECORD_CHUNKS_KAFKA_HANDLER_UUID, event.getId(), TENANT_ID)).thenReturn(Future.succeededFuture());

    // when
    Future<String> future = storedRecordChunksKafkaHandler.handle(kafkaRecord);

    // then
    assertTrue(future.failed());
  }

  @Test
  public void shouldReturnFailedFutureWhenMappingRuleCacheReturnFailed() throws IOException{
    Record record = Json.decodeValue(TestUtil.readFileFromPath(EDIFACT_RECORD_PATH), Record.class);

    RecordsBatchResponse savedRecordsBatch = new RecordsBatchResponse()
      .withRecords(List.of(record))
      .withTotalRecords(1);

    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(Json.encode(savedRecordsBatch));

    when(kafkaRecord.value()).thenReturn(Json.encode(event).getBytes(StandardCharsets.UTF_8));
    when(kafkaRecord.headers()).thenReturn(List.of(KafkaHeader.header(OKAPI_HEADER_TENANT.toLowerCase(), TENANT_ID), KafkaHeader.header("jobExecutionId", UUID.randomUUID().toString())));
    when(eventProcessedService.collectData(STORED_RECORD_CHUNKS_KAFKA_HANDLER_UUID, event.getId(), TENANT_ID)).thenReturn(Future.succeededFuture());
    when(mappingRuleCache.get(new MappingRuleCacheKey(TENANT_ID, EntityType.EDIFACT))).thenReturn(Future.failedFuture(new Exception()));

    // when
    Future<String> future = storedRecordChunksKafkaHandler.handle(kafkaRecord);

    // then
    assertTrue(future.failed());
  }

  @Test
  public void shouldReturnFailedFutureWhenJobExecutionIdIsEmpty() throws IOException{
    Record record = Json.decodeValue(TestUtil.readFileFromPath(EDIFACT_RECORD_PATH), Record.class);

    RecordsBatchResponse savedRecordsBatch = new RecordsBatchResponse()
      .withRecords(List.of(record))
      .withTotalRecords(1);

    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(Json.encode(savedRecordsBatch));

    when(kafkaRecord.headers()).thenReturn(List.of(KafkaHeader.header(OKAPI_HEADER_TENANT.toLowerCase(), TENANT_ID)));
    when(jobExecutionService.getJobExecutionById(any(), any())).thenReturn(Future.succeededFuture(Optional.empty()));

    // when
    Future<String> future = storedRecordChunksKafkaHandler.handle(kafkaRecord);

    // then
    assertTrue(future.failed());
  }

  private void writeSavedRecordsInfoToImportJournal(String marcBibRecordPath, EntityType entityType)
    throws IOException {
    // given
    Record record = Json.decodeValue(TestUtil.readFileFromPath(marcBibRecordPath), Record.class);

    RecordsBatchResponse savedRecordsBatch = new RecordsBatchResponse()
      .withRecords(List.of(record))
      .withTotalRecords(1);

    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(Json.encode(savedRecordsBatch));

    when(kafkaRecord.value()).thenReturn(Json.encode(event).getBytes(StandardCharsets.UTF_8));
    when(kafkaRecord.headers()).thenReturn(List.of(KafkaHeader.header(OKAPI_HEADER_TENANT.toLowerCase(), TENANT_ID),
      KafkaHeader.header(OKAPI_HEADER_TOKEN.toLowerCase(), TOKEN),
      KafkaHeader.header("jobExecutionId", UUID.randomUUID().toString())));    when(eventProcessedService.collectData(STORED_RECORD_CHUNKS_KAFKA_HANDLER_UUID, event.getId(), TENANT_ID)).thenReturn(Future.succeededFuture());
    when(mappingRuleCache.get(new MappingRuleCacheKey(TENANT_ID, entityType))).thenReturn(Future.succeededFuture(Optional.of(mappingRules)));
    when(recordsPublishingService
      .sendEventsWithRecords(anyList(), anyString(), any(OkapiConnectionParams.class), anyString(), any()))
      .thenReturn(Future.succeededFuture(true));

    // when
    Future<String> future = storedRecordChunksKafkaHandler.handle(kafkaRecord);

    // then
    assertTrue(future.succeeded());
    verify(messageProducer, times(1)).write(any());
    verify(recordsPublishingService).sendEventsWithRecords(anyList(), anyString(), argThat(params -> StringUtils.isNotEmpty(params.getToken())), anyString(), any());
  }

  @Test
  public void shouldNotHandleEventWhenJobExecutionWasCancelled() {
    when(kafkaRecord.headers()).thenReturn(List.of(KafkaHeader.header(OKAPI_HEADER_TENANT.toLowerCase(), TENANT_ID)));
    when(jobExecutionService.getJobExecutionById(any(), any())).thenReturn(Future.succeededFuture(Optional.of(new JobExecution().withStatus(JobExecution.Status.CANCELLED))));

    // when
    Future<String> future = storedRecordChunksKafkaHandler.handle(kafkaRecord);

    // then
    verify(recordsPublishingService, never()).sendEventsWithRecords(anyList(), anyString(), any(OkapiConnectionParams.class), anyString(), any());
    assertTrue(future.succeeded());
  }

}
