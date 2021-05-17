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
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.services.MappingRuleCache;
import org.folio.services.RecordsPublishingService;
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
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.folio.rest.RestVerticle.OKAPI_HEADER_TENANT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StoredRecordChunksKafkaHandlerTest {

  private static final String RECORD_PATH = "src/test/resources/org/folio/rest/record.json";
  private static final String MAPPING_RULES_PATH = "src/test/resources/org/folio/services/rules.json";
  private static final String TENANT_ID = "diku";

  private static JsonObject mappingRules;

  @Mock
  private RecordsPublishingService recordsPublishingService;
  @Mock
  private KafkaInternalCache kafkaInternalCache;
  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;
  @Mock
  private JournalService journalService;
  @Mock
  private MappingRuleCache mappingRuleCache;
  @Captor
  private ArgumentCaptor<JsonArray> journalRecordsCaptor;

  private Vertx vertx = Vertx.vertx();
  private AsyncRecordHandler<String, String> storedRecordChunksKafkaHandler;

  @BeforeClass
  public static void setUpClass() throws IOException {
    mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
  }

  @Before
  public void setUp() {
    storedRecordChunksKafkaHandler = new StoredRecordChunksKafkaHandler(recordsPublishingService, journalService, kafkaInternalCache, mappingRuleCache, vertx);
  }

  @Test
  public void shouldNotHandleEventWhenKafkaCacheContainsEventId() throws IOException {
    // given
    RecordsBatchResponse recordsBatch = new RecordsBatchResponse()
      .withRecords(List.of(new Record()))
      .withTotalRecords(1);

    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(ZIPArchiver.zip(Json.encode(recordsBatch)));

    String expectedKafkaRecordKey = "1";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));
    when(kafkaInternalCache.containsByKey(eq(event.getId()))).thenReturn(true);

    // when
    Future<String> future = storedRecordChunksKafkaHandler.handle(kafkaRecord);

    // then
    verify(kafkaInternalCache, times(1)).containsByKey(eq(event.getId()));
    verify(recordsPublishingService, never()).sendEventsWithRecords(anyList(), anyString(), any(OkapiConnectionParams.class), anyString());
    assertTrue(future.succeeded());
    assertEquals(expectedKafkaRecordKey, future.result());
  }

  @Test
  public void shouldWriteSavedRecordsInfoToImportJournal() throws IOException {
    // given
    Record record = Json.decodeValue(TestUtil.readFileFromPath(RECORD_PATH), Record.class);

    RecordsBatchResponse savedRecordsBatch = new RecordsBatchResponse()
      .withRecords(List.of(record))
      .withTotalRecords(1);

    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(ZIPArchiver.zip(Json.encode(savedRecordsBatch)));

    when(kafkaRecord.value()).thenReturn(Json.encode(event));
    when(kafkaRecord.headers()).thenReturn(List.of(KafkaHeader.header(OKAPI_HEADER_TENANT, TENANT_ID)));
    when(kafkaInternalCache.containsByKey(eq(event.getId()))).thenReturn(false);
    when(mappingRuleCache.get(TENANT_ID)).thenReturn(Future.succeededFuture(Optional.of(mappingRules)));
    when(recordsPublishingService.sendEventsWithRecords(anyList(), isNull(), any(OkapiConnectionParams.class), anyString()))
      .thenReturn(Future.succeededFuture(true));

    // when
    Future<String> future = storedRecordChunksKafkaHandler.handle(kafkaRecord);

    // then
    assertTrue(future.succeeded());
    verify(journalService, times(1)).saveBatch(journalRecordsCaptor.capture(), eq(TENANT_ID));

    assertEquals(1, journalRecordsCaptor.getValue().size());
    JournalRecord journalRecord = journalRecordsCaptor.getValue().getJsonObject(0).mapTo(JournalRecord.class);
    assertEquals(record.getId(), journalRecord.getSourceId());
    assertEquals(JournalRecord.EntityType.MARC_BIBLIOGRAPHIC, journalRecord.getEntityType());
    assertEquals(JournalRecord.ActionType.CREATE, journalRecord.getActionType());
    assertEquals(JournalRecord.ActionStatus.COMPLETED, journalRecord.getActionStatus());
    assertEquals("The Journal of ecclesiastical history.", journalRecord.getTitle());
  }

}
