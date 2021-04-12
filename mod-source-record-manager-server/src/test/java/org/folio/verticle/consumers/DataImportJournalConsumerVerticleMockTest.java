package org.folio.verticle.consumers;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.folio.DataImportEventPayload;
import org.folio.dao.JournalRecordDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.JournalRecord.ActionStatus;
import org.folio.rest.jaxrs.model.JournalRecord.EntityType;
import org.folio.services.journal.JournalServiceImpl;
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
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_COMPLETED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDING_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.services.journal.JournalUtil.ERROR_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class DataImportJournalConsumerVerticleMockTest extends AbstractRestTest {

  public static final String ENTITY_TYPE_KEY = "entityType";
  public static final String ACTION_TYPE_KEY = "actionType";
  public static final String ACTION_STATUS_KEY = "actionStatus";
  public static final String SOURCE_RECORD_ID_KEY = "sourceId";
  public static final String ENV_KEY = "folio";

  @Spy
  private Vertx vertx = Vertx.vertx();

  @InjectMocks
  private JournalRecordDaoImpl journalRecordDao;

  @Spy
  private final PostgresClientFactory postgresClientFactory = new PostgresClientFactory(Vertx.vertx());

  @Mock
  private KafkaInternalCache kafkaInternalCache;

  @Spy
  private final JournalServiceImpl journalService = new JournalServiceImpl(journalRecordDao);

  @Captor
  private ArgumentCaptor<JsonObject> journalRecordCaptor;

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

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    dataImportJournalKafkaHandler = new DataImportJournalKafkaHandler(vertx, kafkaInternalCache, journalService);
    when(kafkaInternalCache.containsByKey(anyString())).thenReturn(false);
  }

  @Test
  public void shouldProcessEvent(TestContext context) throws IOException {
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

    Mockito.doNothing().when(journalService).save(ArgumentMatchers.any(JsonObject.class), ArgumentMatchers.any(String.class));

    KafkaConsumerRecord<String, String> kafkaConsumerRecord = buildKafkaConsumerRecord(dataImportEventPayload);
    dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    Mockito.verify(journalService).save(journalRecordCaptor.capture(), Mockito.anyString());

    JsonObject jsonObject = journalRecordCaptor.getValue();
    Assert.assertEquals("Entity Type:", EntityType.INSTANCE.value(), jsonObject.getString(ENTITY_TYPE_KEY));
    Assert.assertEquals("Action Type:", ActionType.CREATE.value(), jsonObject.getString(ACTION_TYPE_KEY));
    Assert.assertEquals("Action Status:", ActionStatus.COMPLETED.value(), jsonObject.getString(ACTION_STATUS_KEY));

    async.complete();
  }

  @Test
  public void shouldProcessCompletedEvent(TestContext context) throws IOException {
    Async async = context.async();

    HashMap<String, String> dataImportEventPayloadContext = new HashMap<>() {{
      put(HOLDINGS.value(), recordJson.encode());
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

    Mockito.doNothing().when(journalService).save(ArgumentMatchers.any(JsonObject.class), ArgumentMatchers.any(String.class));

    KafkaConsumerRecord<String, String> kafkaConsumerRecord = buildKafkaConsumerRecord(dataImportEventPayload);
    dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    Mockito.verify(journalService).save(journalRecordCaptor.capture(), Mockito.anyString());

    JsonObject jsonObject = journalRecordCaptor.getValue();
    Assert.assertEquals("Entity Type:", HOLDINGS.value(), jsonObject.getString(ENTITY_TYPE_KEY));
    Assert.assertEquals("Action Type:", ActionType.UPDATE.value(), jsonObject.getString(ACTION_TYPE_KEY));
    Assert.assertEquals("Action Status:", ActionStatus.COMPLETED.value(), jsonObject.getString(ACTION_STATUS_KEY));

    async.complete();
  }

  @Test
  public void shouldProcessErrorEvent(TestContext context) throws IOException {
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

    Mockito.doNothing().when(journalService).save(ArgumentMatchers.any(JsonObject.class), ArgumentMatchers.any(String.class));

    KafkaConsumerRecord<String, String> kafkaConsumerRecord = buildKafkaConsumerRecord(dataImportEventPayload);
    dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    Mockito.verify(journalService).save(journalRecordCaptor.capture(), Mockito.anyString());

    JsonObject jsonObject = journalRecordCaptor.getValue();
    Assert.assertEquals("Entity Type:", EntityType.INSTANCE.value(), jsonObject.getString(ENTITY_TYPE_KEY));
    Assert.assertEquals("Action Type:", ActionType.CREATE.value(), jsonObject.getString(ACTION_TYPE_KEY));
    Assert.assertEquals("Action Status:", ActionStatus.ERROR.value(), jsonObject.getString(ACTION_STATUS_KEY));

    async.complete();
  }

  @Test
  public void shouldProcessEventWhenKafkaCacheContainsEventId() throws IOException {
    // given
    when(kafkaInternalCache.containsByKey(anyString())).thenReturn(true);
    Mockito.doNothing().when(journalService).save(ArgumentMatchers.any(JsonObject.class), ArgumentMatchers.any(String.class));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withJobExecutionId(jobExecution.getId())
      .withContext(new HashMap<>())
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID);

    // when
    KafkaConsumerRecord<String, String> kafkaConsumerRecord = buildKafkaConsumerRecord(dataImportEventPayload);
    dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    // then
    verify(kafkaInternalCache, times(1)).containsByKey(anyString());
    verify(journalService, never()).save(any(JsonObject.class), eq(TENANT_ID));
  }

  private KafkaConsumerRecord<String, String> buildKafkaConsumerRecord(DataImportEventPayload record) throws IOException {
    String topic = KafkaTopicNameHelper.formatTopicName(ENV_KEY, getDefaultNameSpace(), TENANT_ID, record.getEventType());
    Event event = new Event().withId(UUID.randomUUID().toString()).withEventPayload(ZIPArchiver.zip(Json.encode(record)));
    ConsumerRecord<String, String> consumerRecord = buildConsumerRecord(topic, event);
    return new KafkaConsumerRecordImpl<>(consumerRecord);
  }

  private ConsumerRecord<String, String> buildConsumerRecord(String topic, Event event) {
    ConsumerRecord<java.lang.String, java.lang.String> consumerRecord =
      new ConsumerRecord(ENV_KEY, 0, 0, topic, Json.encode(event));
    consumerRecord.headers().add(new RecordHeader(OKAPI_TENANT_HEADER, TENANT_ID.getBytes(StandardCharsets.UTF_8)));
    consumerRecord.headers().add(new RecordHeader(OKAPI_URL_HEADER, ("http://localhost:" + snapshotMockServer.port()).getBytes(StandardCharsets.UTF_8)));
    consumerRecord.headers().add(new RecordHeader(OKAPI_TOKEN_HEADER, (TOKEN).getBytes(StandardCharsets.UTF_8)));
    return consumerRecord;
  }

}
