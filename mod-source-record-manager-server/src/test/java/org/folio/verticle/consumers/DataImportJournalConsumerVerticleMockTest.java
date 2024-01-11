package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import io.vertx.pgclient.PgException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.folio.DataImportEventPayload;
import org.folio.TestUtil;
import org.folio.dao.JournalRecordDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.JournalRecord.ActionStatus;
import org.folio.rest.jaxrs.model.JournalRecord.EntityType;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.EventProcessedService;
import org.folio.services.MappingRuleCache;
import org.folio.services.journal.JournalServiceImpl;
import static org.folio.verticle.consumers.DataImportJournalKafkaHandler.DATA_IMPORT_JOURNAL_KAFKA_HANDLER_UUID;
import org.folio.verticle.consumers.util.EventTypeHandlerSelector;
import org.folio.verticle.consumers.util.MarcImportEventsHandler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_COMPLETED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDING_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.services.journal.JournalUtil.ERROR_KEY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertTrue;

@RunWith(VertxUnitRunner.class)
public class DataImportJournalConsumerVerticleMockTest extends AbstractRestTest {

  public static final String ENTITY_TYPE_KEY = "entityType";
  public static final String ACTION_TYPE_KEY = "actionType";
  public static final String ACTION_STATUS_KEY = "actionStatus";
  public static final String SOURCE_RECORD_ID_KEY = "sourceId";
  public static final String ENV_KEY = "folio";
  public static final String RECORD_PATH = "src/test/resources/org/folio/rest/record.json";
  public static final String MAPPING_RULES_PATH = "src/test/resources/org/folio/services/marc_bib_rules.json";

  @Spy
  private Vertx vertx = Vertx.vertx();

  @InjectMocks
  private JournalRecordDaoImpl journalRecordDao;

  @Spy
  private final PostgresClientFactory postgresClientFactory = new PostgresClientFactory(Vertx.vertx());

  @Mock
  private EventProcessedService eventProcessedService;

  @Mock
  private MappingRuleCache mappingRuleCache;

  @Spy
  @InjectMocks
  private MarcImportEventsHandler marcImportEventsHandler;

  @Spy
  private final JournalServiceImpl journalService = new JournalServiceImpl(journalRecordDao);

  @Captor
  private ArgumentCaptor<JsonArray> journalRecordCaptor;

  private DataImportJournalKafkaHandler dataImportJournalKafkaHandler;

  private final String jobExecutionUUID = "5105b55a-b9a3-4f76-9402-a5243ea63c95";

  private final JobExecution jobExecution = new JobExecution()
    .withId(jobExecutionUUID)
    .withHrId(1000)
    .withParentJobId(jobExecutionUUID)
    .withSubordinationType(JobExecution.SubordinationType.PARENT_SINGLE)
    .withStatus(JobExecution.Status.NEW)
    .withUiStatus(JobExecution.UiStatus.INITIALIZATION)
    .withSourcePath("importMarc.mrc")
    .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"))
    .withUserId(UUID.randomUUID().toString());

  private final JsonObject recordJson = new JsonObject()
    .put("id", UUID.randomUUID().toString())
    .put("snapshotId", jobExecutionUUID)
    .put("order", 1);

  private Record record;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.initMocks(this);
    EventTypeHandlerSelector eventTypeHandlerSelector = new EventTypeHandlerSelector(marcImportEventsHandler);
    dataImportJournalKafkaHandler = new DataImportJournalKafkaHandler(vertx, eventProcessedService, eventTypeHandlerSelector, journalService);
    record = Json.decodeValue(TestUtil.readFileFromPath(RECORD_PATH), Record.class);
    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    when(mappingRuleCache.get(any())).thenReturn(Future.succeededFuture(Optional.of(mappingRules)));
    when(eventProcessedService.collectData(eq(DATA_IMPORT_JOURNAL_KAFKA_HANDLER_UUID), anyString(), eq(TENANT_ID)))
      .thenReturn(Future.succeededFuture());
  }

  @Test
  public void shouldProcessEvent(TestContext context) {
    Async async = context.async();

    HashMap<String, String> dataImportEventPayloadContext = new HashMap<>() {{
      put(INSTANCE.value(), recordJson.encode());
      put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    }};

    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withProfileSnapshot(profileSnapshotWrapperResponse)
      .withCurrentNode(profileSnapshotWrapperResponse.getChildSnapshotWrappers().get(0))
      .withJobExecutionId(jobExecution.getId())
      .withContext(dataImportEventPayloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken("token");

    Mockito.doNothing().when(journalService).saveBatch(ArgumentMatchers.any(JsonArray.class), ArgumentMatchers.any(String.class));

    KafkaConsumerRecord<String, byte[]> kafkaConsumerRecord = buildKafkaConsumerRecord(dataImportEventPayload);
    dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    Mockito.verify(journalService).saveBatch(journalRecordCaptor.capture(), Mockito.anyString());

    JsonArray jsonArray = journalRecordCaptor.getValue();
    Assert.assertEquals("Entity Type:", EntityType.INSTANCE.value(), jsonArray.getJsonObject(0).getString(ENTITY_TYPE_KEY));
    Assert.assertEquals("Action Type:", ActionType.CREATE.value(), jsonArray.getJsonObject(0).getString(ACTION_TYPE_KEY));
    Assert.assertEquals("Action Status:", ActionStatus.COMPLETED.value(), jsonArray.getJsonObject(0).getString(ACTION_STATUS_KEY));

    async.complete();
  }

  @Test
  public void shouldProcessCompletedEvent(TestContext context) {
    Async async = context.async();

    JsonObject holdingsJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("snapshotId", jobExecutionUUID)
      .put("order", 1);

    JsonArray multipleHoldings = new JsonArray();
    multipleHoldings.add(holdingsJson);
    HashMap<String, String> dataImportEventPayloadContext = new HashMap<>() {{
      put(HOLDINGS.value(), String.valueOf(multipleHoldings));
      put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    }};

    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_COMPLETED.value())
      .withProfileSnapshot(profileSnapshotWrapperResponse)
      .withCurrentNode(profileSnapshotWrapperResponse.getChildSnapshotWrappers().get(0))
      .withJobExecutionId(jobExecution.getId())
      .withContext(dataImportEventPayloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken("token")
      .withEventsChain(List.of(DI_INVENTORY_HOLDING_UPDATED.value()));

    Mockito.doNothing().when(journalService).saveBatch(ArgumentMatchers.any(JsonArray.class), ArgumentMatchers.any(String.class));

    KafkaConsumerRecord<String, byte[]> kafkaConsumerRecord = buildKafkaConsumerRecord(dataImportEventPayload);
    dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    Mockito.verify(journalService).saveBatch(journalRecordCaptor.capture(), Mockito.anyString());

    JsonArray jsonArray = journalRecordCaptor.getValue();
    Assert.assertEquals("Entity Type:", HOLDINGS.value(), jsonArray.getJsonObject(0).getString(ENTITY_TYPE_KEY));
    Assert.assertEquals("Action Type:", ActionType.UPDATE.value(), jsonArray.getJsonObject(0).getString(ACTION_TYPE_KEY));
    Assert.assertEquals("Action Status:", ActionStatus.COMPLETED.value(), jsonArray.getJsonObject(0).getString(ACTION_STATUS_KEY));

    async.complete();
  }

  @Test
  public void shouldProcessErrorEvent(TestContext context) {
    Async async = context.async();

    HashMap<String, String> dataImportEventPayloadContext = new HashMap<>() {{
      put(INSTANCE.value(), recordJson.encode());
      put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
      put(ERROR_KEY, "Error creating inventory Instance.");
    }};

    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withProfileSnapshot(profileSnapshotWrapperResponse)
      .withCurrentNode(profileSnapshotWrapperResponse.getChildSnapshotWrappers().get(0))
      .withJobExecutionId(jobExecution.getId())
      .withContext(dataImportEventPayloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken("token")
      .withEventsChain(List.of(DI_INVENTORY_INSTANCE_CREATED.value()));

    Mockito.doNothing().when(journalService).saveBatch(ArgumentMatchers.any(JsonArray.class), ArgumentMatchers.any(String.class));

    KafkaConsumerRecord<String, byte[]> kafkaConsumerRecord = buildKafkaConsumerRecord(dataImportEventPayload);
    dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    Mockito.verify(journalService).saveBatch(journalRecordCaptor.capture(), Mockito.anyString());

    JsonArray jsonArray = journalRecordCaptor.getValue();
    Assert.assertEquals("Entity Type:", EntityType.INSTANCE.value(), jsonArray.getJsonObject(0).getString(ENTITY_TYPE_KEY));
    Assert.assertEquals("Action Type:", ActionType.CREATE.value(), jsonArray.getJsonObject(0).getString(ACTION_TYPE_KEY));
    Assert.assertEquals("Action Status:", ActionStatus.ERROR.value(), jsonArray.getJsonObject(0).getString(ACTION_STATUS_KEY));

    async.complete();
  }

  @Test
  public void shouldProcessErrorEventAsSourceRecordErrorWhenEventChainHasNoEvents() {
    // given
    HashMap<String, String> dataImportEventPayloadContext = new HashMap<>() {{
      put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
      put(ERROR_KEY, "java.lang.IllegalStateException: Unsupported Marc record type");
    }};

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withJobExecutionId(jobExecution.getId())
      .withContext(dataImportEventPayloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken("token");

    Mockito.doNothing().when(journalService).saveBatch(ArgumentMatchers.any(JsonArray.class), ArgumentMatchers.any(String.class));

    // when
    KafkaConsumerRecord<String, byte[]> kafkaConsumerRecord = buildKafkaConsumerRecord(dataImportEventPayload);
    dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    // then
    Mockito.verify(journalService).saveBatch(journalRecordCaptor.capture(), eq(TENANT_ID));

    JsonArray jsonArray = journalRecordCaptor.getValue();
    Assert.assertEquals("Entity Type:", EntityType.MARC_BIBLIOGRAPHIC.value(), jsonArray.getJsonObject(0).getString(ENTITY_TYPE_KEY));
    Assert.assertEquals("Action Type:", ActionType.CREATE.value(), jsonArray.getJsonObject(0).getString(ACTION_TYPE_KEY));
    Assert.assertEquals("Action Status:", ActionStatus.ERROR.value(), jsonArray.getJsonObject(0).getString(ACTION_STATUS_KEY));
    Assert.assertEquals("Source Record id:", recordJson.getString("id"), jsonArray.getJsonObject(0).getString(SOURCE_RECORD_ID_KEY));
    Assert.assertNotNull(jsonArray.getJsonObject(0).getString("error"));
  }

  @Test
  public void shouldFillTitleOnRecordModifiedEventProcessing() {
    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED.value())
      .withJobExecutionId(jobExecution.getId())
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
      }});

    Mockito.doNothing().when(journalService).saveBatch(ArgumentMatchers.any(JsonArray.class), ArgumentMatchers.any(String.class));

    // when
    KafkaConsumerRecord<String, byte[]> kafkaConsumerRecord = buildKafkaConsumerRecord(dataImportEventPayload);
    dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    // then
    Mockito.verify(journalService).saveBatch(journalRecordCaptor.capture(), eq(TENANT_ID));

    JournalRecord journalRecord = journalRecordCaptor.getValue().getJsonObject(0).mapTo(JournalRecord.class);
    Assert.assertEquals("Entity Type:", EntityType.MARC_BIBLIOGRAPHIC, journalRecord.getEntityType());
    Assert.assertEquals("Action Type:", ActionType.MODIFY, journalRecord.getActionType());
    Assert.assertEquals("Action Status:", ActionStatus.COMPLETED, journalRecord.getActionStatus());
    Assert.assertEquals("Title:", "The Journal of ecclesiastical history.", journalRecord.getTitle());
  }

  @Test
  public void shouldNotProcessEventWhenItAlreadyProcessed() {
    // given
    when(eventProcessedService.collectData(eq(DATA_IMPORT_JOURNAL_KAFKA_HANDLER_UUID), anyString(), eq(TENANT_ID)))
      .thenReturn(Future.failedFuture(new DuplicateEventException("ConstraintViolation occurs")));
    Mockito.doNothing().when(journalService).saveBatch(ArgumentMatchers.any(JsonArray.class), ArgumentMatchers.any(String.class));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withJobExecutionId(jobExecution.getId())
      .withContext(new HashMap<>())
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID);

    // when
    KafkaConsumerRecord<String, byte[]> kafkaConsumerRecord = buildKafkaConsumerRecord(dataImportEventPayload);
    Future<String> future = dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    // then
    verify(journalService, never()).saveBatch(any(JsonArray.class), eq(TENANT_ID));
    assertTrue(future.succeeded());
  }

  @Test
  public void shouldFailWhenErrorWithDbOccurs() {
    // given
    when(eventProcessedService.collectData(eq(DATA_IMPORT_JOURNAL_KAFKA_HANDLER_UUID), anyString(), eq(TENANT_ID)))
      .thenReturn(Future.failedFuture(new PgException("Connection timeout!", "ERROR", "ERROR", "ERROR")));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withJobExecutionId(jobExecution.getId())
      .withContext(new HashMap<>())
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID);

    // when
    KafkaConsumerRecord<String, byte[]> kafkaConsumerRecord = buildKafkaConsumerRecord(dataImportEventPayload);
    Future<String> future = dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    // then
    verify(journalService, never()).saveBatch(any(JsonArray.class), eq(TENANT_ID));
    assertTrue(future.failed());
    assertTrue(future.cause() instanceof PgException);
  }

  private KafkaConsumerRecord<String, byte[]> buildKafkaConsumerRecord(DataImportEventPayload record) {
    String topic = KafkaTopicNameHelper.formatTopicName(ENV_KEY, getDefaultNameSpace(), TENANT_ID, record.getEventType());
    Event event = new Event().withId(UUID.randomUUID().toString()).withEventPayload(Json.encode(record));
    ConsumerRecord<String, byte[]> consumerRecord = buildConsumerRecord(topic, event);
    return new KafkaConsumerRecordImpl<>(consumerRecord);
  }
}
