package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.folio.TestUtil;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.JournalRecord.EntityType;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.services.EventProcessedService;
import org.folio.services.JobExecutionService;
import org.folio.services.MappingRuleCache;
import org.folio.services.RecordsPublishingService;
import org.folio.services.entity.MappingRuleCacheKey;
import org.folio.services.journal.JournalService;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.folio.rest.RestVerticle.OKAPI_HEADER_TENANT;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.folio.verticle.consumers.StoredRecordChunksKafkaHandler.STORED_RECORD_CHUNKS_KAFKA_HANDLER_UUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class StoredRecordChunksKafkaHandlerTest {

  private static final String MARC_BIB_RECORD_PATH = "src/test/resources/org/folio/rest/record.json";
  private static final String MARC_AUTHORITY_RECORD_PATH = "src/test/resources/org/folio/rest/marcAuthorityRecord.json";
  private static final String MARC_HOLDING_RECORD_PATH = "src/test/resources/org/folio/rest/marcHoldingRecord.json";
  private static final String EDIFACT_RECORD_PATH = "src/test/resources/org/folio/rest/edifactRecord.json";
  private static final String MAPPING_RULES_PATH = "src/test/resources/org/folio/services/marc_bib_rules.json";
  private static final String TENANT_ID = "diku";

  private static JsonObject mappingRules;

  @Mock
  private RecordsPublishingService recordsPublishingService;
  @Mock
  private KafkaConsumerRecord<String, byte[]> kafkaRecord;
  @Mock
  private JournalService journalService;
  @Mock
  private EventProcessedService eventProcessedService;
  @Mock
  private MappingRuleCache mappingRuleCache;
  @Mock
  private JobExecutionService jobExecutionService;
  @Captor
  private ArgumentCaptor<JsonArray> journalRecordsCaptor;

  private Vertx vertx = Vertx.vertx();
  private AsyncRecordHandler<String, byte[]> storedRecordChunksKafkaHandler;

  @BeforeClass
  public static void setUpClass() throws IOException {
    mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
  }

  @Before
  public void setUp() {
    storedRecordChunksKafkaHandler = new StoredRecordChunksKafkaHandler(recordsPublishingService, journalService, eventProcessedService,jobExecutionService, mappingRuleCache,  vertx);
    when(jobExecutionService.getJobExecutionById(anyString(), anyString()))
      .thenReturn(Future.succeededFuture(Optional.of(new JobExecution())));
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
    verify(recordsPublishingService, never()).sendEventsWithRecords(anyList(), anyString(), any(OkapiConnectionParams.class), anyString());
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
    when(kafkaRecord.headers()).thenReturn(List.of(KafkaHeader.header(OKAPI_HEADER_TENANT.toLowerCase(), TENANT_ID), KafkaHeader.header("jobExecutionId", UUID.randomUUID().toString())));
    when(eventProcessedService.collectData(STORED_RECORD_CHUNKS_KAFKA_HANDLER_UUID, event.getId(), TENANT_ID)).thenReturn(Future.succeededFuture());
    when(mappingRuleCache.get(new MappingRuleCacheKey(TENANT_ID, entityType))).thenReturn(Future.succeededFuture(Optional.of(mappingRules)));
    when(recordsPublishingService
      .sendEventsWithRecords(anyList(), anyString(), any(OkapiConnectionParams.class), anyString()))
      .thenReturn(Future.succeededFuture(true));

    // when
    Future<String> future = storedRecordChunksKafkaHandler.handle(kafkaRecord);

    // then
    assertTrue(future.succeeded());
    verify(journalService, times(1)).saveBatch(journalRecordsCaptor.capture(), eq(TENANT_ID));

    assertEquals(1, journalRecordsCaptor.getValue().size());
    JournalRecord journalRecord = journalRecordsCaptor.getValue().getJsonObject(0).mapTo(JournalRecord.class);
    assertEquals(record.getId(), journalRecord.getSourceId());
    assertEquals(entityType, journalRecord.getEntityType());
    assertEquals(JournalRecord.ActionType.CREATE, journalRecord.getActionType());
    assertEquals(JournalRecord.ActionStatus.COMPLETED, journalRecord.getActionStatus());
    assertEquals("The Journal of ecclesiastical history.", journalRecord.getTitle());
  }

  @Test
  public void shouldNotHandleEventWhenJobExecutionWasCancelled() {
    when(kafkaRecord.headers()).thenReturn(List.of(KafkaHeader.header(OKAPI_HEADER_TENANT.toLowerCase(), TENANT_ID)));
    when(jobExecutionService.getJobExecutionById(any(), any())).thenReturn(Future.succeededFuture(Optional.of(new JobExecution().withStatus(JobExecution.Status.CANCELLED))));

    // when
    Future<String> future = storedRecordChunksKafkaHandler.handle(kafkaRecord);

    // then
    verify(recordsPublishingService, never()).sendEventsWithRecords(anyList(), anyString(), any(OkapiConnectionParams.class), anyString());
    assertTrue(future.succeeded());
  }

}
