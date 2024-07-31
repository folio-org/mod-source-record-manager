package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.SendKeyValues;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.TestUtil;
import org.folio.dao.JobExecutionDaoImpl;
import org.folio.dao.JournalRecordDao;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.EventProcessedService;
import org.folio.services.EventProcessedServiceImpl;
import org.folio.services.journal.JournalService;
import org.folio.verticle.consumers.util.EventTypeHandlerSelector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.*;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.services.journal.JournalUtil.ERROR_KEY;
import static org.folio.verticle.consumers.DataImportKafkaHandler.JOB_EXECUTION_ID_HEADER;


@RunWith(VertxUnitRunner.class)
public class DataImportJournalConsumerVerticleTest extends AbstractRestTest {

  private static final Logger LOGGER = LogManager.getLogger();

  private EventProcessedService eventProcessedService;
  private JournalService journalService;
  private JobExecutionDaoImpl jobExecutionDao;
  private JournalRecordDao journalRecordDao;
  private String jobExecutionUUID;
  private JobExecution jobExecution;
  private JsonObject recordJson;
  private HashMap<String, String> dataImportEventPayloadContext;

  @Before
  public void setUp(TestContext context) {
    jobExecutionUUID = UUID.randomUUID().toString();
    jobExecution = new JobExecution()
      .withId(jobExecutionUUID)
      .withHrId(1000)
      .withParentJobId(jobExecutionUUID)
      .withSubordinationType(JobExecution.SubordinationType.PARENT_SINGLE)
      .withStatus(JobExecution.Status.NEW)
      .withUiStatus(JobExecution.UiStatus.INITIALIZATION)
      .withSourcePath("importMarc.mrc")
      .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"))
      .withUserId(UUID.randomUUID().toString());
    recordJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("snapshotId", jobExecutionUUID)
      .put("order", 1);
    dataImportEventPayloadContext = new HashMap<>() {{
      put(INSTANCE.value(), recordJson.encode());
      put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    }};

    jobExecutionDao = getBeanFromSpringContext(vertx, org.folio.dao.JobExecutionDaoImpl.class);
    Assert.assertNotNull(jobExecutionDao);
    jobExecutionDao.save(jobExecution, TENANT_ID);

    journalRecordDao = getBeanFromSpringContext(vertx, org.folio.dao.JournalRecordDaoImpl.class);
    Assert.assertNotNull(journalRecordDao);

    journalService = getBeanFromSpringContext(vertx, org.folio.services.journal.JournalServiceImpl.class);
    Assert.assertNotNull(journalService);

    eventProcessedService = getBeanFromSpringContext(vertx, EventProcessedServiceImpl.class);
    Assert.assertNotNull(eventProcessedService);

    EventTypeHandlerSelector eventTypeHandlerSelector = getBeanFromSpringContext(vertx, EventTypeHandlerSelector.class);
    Assert.assertNotNull(eventTypeHandlerSelector);
  }

  private void assertJournalRecord(TestContext context, String jobExecutionId) {
    assertJournalRecord(context, jobExecutionId, (records) -> true);
  }

  @SuppressWarnings("java:S5779")
  private void assertJournalRecord(TestContext context, String jobExecutionId, Predicate<Collection<JournalRecord>> valueChecker) {
    Async async = context.async();
    long timerId = vertx.setTimer(60_000, id -> {
      context.fail("Failed to assert presence of journal records for jobExecutionId=" + jobExecutionUUID);
    });

    vertx.setPeriodic(3000, id -> {
      // Your assertion here
      Future<List<JournalRecord>> future = journalRecordDao.getByJobExecutionId(jobExecutionId, "action_type", "asc", TENANT_ID);
      future.onComplete(ar -> {
        try {
          context.assertTrue(ar.succeeded());
          List<JournalRecord> result = ar.result();
          Assert.assertNotNull(result);
          Assert.assertFalse(result.isEmpty());
          Assert.assertTrue(valueChecker.test(result));
          vertx.cancelTimer(timerId); // Cancel the fail timer
          vertx.cancelTimer(id); // Stop periodic checks
          async.complete();
        }
        catch (java.lang.AssertionError e) {
          LOGGER.warn("Assertion was not successful");
        }
      });
    });
  }

  @Test
  public void testJournalInventoryInstanceCreatedAction(TestContext context) throws InterruptedException {
    Async async = context.async();
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

    // when
    Event event = new Event().withEventPayload(Json.encode(dataImportEventPayload))
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withId(UUID.randomUUID().toString());
    String topic = formatToKafkaTopicName(DI_INVENTORY_INSTANCE_CREATED.value());

    KeyValue<String, String> kafkaRecord = new KeyValue<>("key", Json.encode(event));
    kafkaRecord.addHeader(OKAPI_TENANT_HEADER, TENANT_ID, UTF_8);
    kafkaRecord.addHeader(OKAPI_URL_HEADER, snapshotMockServer.baseUrl(), UTF_8);
    kafkaRecord.addHeader(JOB_EXECUTION_ID_HEADER, jobExecutionUUID, UTF_8);
    SendKeyValues<String, String> request = SendKeyValues.to(topic, Collections.singletonList(kafkaRecord))
      .useDefaults();
    kafkaCluster.send(request);

    // then
    assertJournalRecord(context, jobExecution.getId());
    async.complete();
  }

  @Test
  public void testJournalMarcBibRecordUpdatedAction(TestContext context) throws InterruptedException {
    Async async = context.async();

    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_UPDATED.value())
      .withProfileSnapshot(profileSnapshotWrapperResponse)
      .withCurrentNode(profileSnapshotWrapperResponse.getChildSnapshotWrappers().get(0))
      .withJobExecutionId(jobExecution.getId())
      .withContext(dataImportEventPayloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken(TOKEN);

    // when
    Event event = new Event().withEventPayload(Json.encode(dataImportEventPayload))
      .withEventType(DI_SRS_MARC_BIB_RECORD_UPDATED.value())
      .withId(UUID.randomUUID().toString());
    String topic = formatToKafkaTopicName(DI_SRS_MARC_BIB_RECORD_UPDATED.value());
    KeyValue<String, String> kafkaRecord = new KeyValue<>("key", Json.encode(event));
    kafkaRecord.addHeader(OKAPI_TENANT_HEADER, TENANT_ID, UTF_8);
    kafkaRecord.addHeader(OKAPI_URL_HEADER, snapshotMockServer.baseUrl(), UTF_8);
    kafkaRecord.addHeader(JOB_EXECUTION_ID_HEADER, jobExecutionUUID, UTF_8);
    SendKeyValues<String, String> request = SendKeyValues.to(topic, Collections.singletonList(kafkaRecord))
      .useDefaults();
    kafkaCluster.send(request);

    // then
    assertJournalRecord(context, jobExecution.getId());
    async.complete();
  }

  @Test
  public void testJournalMarcHoldingsRecordCreatedAction(TestContext context) throws InterruptedException {
    Async async = context.async();

    // given
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withProfileSnapshot(profileSnapshotWrapperResponse)
      .withCurrentNode(profileSnapshotWrapperResponse.getChildSnapshotWrappers().get(0))
      .withJobExecutionId(jobExecution.getId())
      .withContext(dataImportEventPayloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken(TOKEN);

    // when
    Event event = new Event().withEventPayload(Json.encode(dataImportEventPayload))
      .withEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED.value())
      .withId(UUID.randomUUID().toString());
    String topic = formatToKafkaTopicName(DI_SRS_MARC_HOLDING_RECORD_CREATED.value());
    KeyValue<String, String> kafkaRecord = new KeyValue<>("key", Json.encode(event));
    kafkaRecord.addHeader(OKAPI_TENANT_HEADER, TENANT_ID, UTF_8);
    kafkaRecord.addHeader(OKAPI_URL_HEADER, snapshotMockServer.baseUrl(), UTF_8);
    kafkaRecord.addHeader(JOB_EXECUTION_ID_HEADER, jobExecutionUUID, UTF_8);
    SendKeyValues<String, String> request = SendKeyValues.to(topic, Collections.singletonList(kafkaRecord))
      .useDefaults();
    kafkaCluster.send(request);

    // then
    assertJournalRecord(context, jobExecution.getId());
    async.complete();
  }

  @Test
  public void testJournalCompletedAction(TestContext context) throws InterruptedException {
    Async async = context.async();

    // given
    DataImportEventPayload completedEventPayload = new DataImportEventPayload()
      .withEventType(DI_COMPLETED.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withJobExecutionId(jobExecution.getId())
      .withContext(new HashMap<>() {{
        put(ITEM.value(), new JsonArray().add(recordJson).encode());
        put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
      }})
      .withProfileSnapshot(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.ITEM))))
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.ITEM))))
      .withEventsChain(List.of(DI_INVENTORY_HOLDING_CREATED.value(), DI_INVENTORY_ITEM_CREATED.value()));

    // when
    Event event = new Event().withEventPayload(Json.encode(completedEventPayload))
      .withEventType(DI_COMPLETED.value())
      .withId(UUID.randomUUID().toString());
    String topic = formatToKafkaTopicName(DI_COMPLETED.value());
    KeyValue<String, String> kafkaRecord = new KeyValue<>("key", Json.encode(event));
    kafkaRecord.addHeader(OKAPI_TENANT_HEADER, TENANT_ID, UTF_8);
    kafkaRecord.addHeader(OKAPI_URL_HEADER, snapshotMockServer.baseUrl(), UTF_8);
    kafkaRecord.addHeader(JOB_EXECUTION_ID_HEADER, jobExecutionUUID, UTF_8);
    SendKeyValues<String, String> request = SendKeyValues.to(topic, Collections.singletonList(kafkaRecord))
      .useDefaults();
    kafkaCluster.send(request);

    // then
    assertJournalRecord(context, jobExecution.getId());
    async.complete();
  }

  @Test
  public void testJournalErrorAction(TestContext context) throws InterruptedException {
    Async async = context.async();

    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withJobExecutionId(jobExecution.getId())
      .withContext(new HashMap<>() {{
        put(HOLDINGS.value(), recordJson.encode());
        put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
        put("ERROR", "java.lang.IllegalArgumentException: Can not handle event payload");
      }})
      .withProfileSnapshot(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.HOLDINGS))))
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(new ActionProfile().withFolioRecord(ActionProfile.FolioRecord.HOLDINGS))))
      .withEventsChain(List.of(DI_INCOMING_MARC_BIB_RECORD_PARSED.value(), DI_INVENTORY_HOLDING_CREATED.value()));

    // when
    Event event = new Event().withEventPayload(Json.encode(eventPayload))
      .withEventType(DI_ERROR.value())
      .withId(UUID.randomUUID().toString());
    String topic = formatToKafkaTopicName(DI_ERROR.value());
    KeyValue<String, String> kafkaRecord = new KeyValue<>("key", Json.encode(event));
    kafkaRecord.addHeader(OKAPI_TENANT_HEADER, TENANT_ID, UTF_8);
    kafkaRecord.addHeader(OKAPI_URL_HEADER, snapshotMockServer.baseUrl(), UTF_8);
    kafkaRecord.addHeader(JOB_EXECUTION_ID_HEADER, jobExecutionUUID, UTF_8);
    SendKeyValues<String, String> request = SendKeyValues.to(topic, Collections.singletonList(kafkaRecord))
      .useDefaults();
    kafkaCluster.send(request);

    // then
    assertJournalRecord(context, jobExecution.getId());
    async.complete();
  }

  @Test
  public void testShouldProcessErrorEventAsSourceRecordErrorWhenEventChainHasNoEvents(TestContext context) throws InterruptedException {
    Async async = context.async();

    // given
    String incomingRecordId = UUID.randomUUID().toString();
    HashMap<String, String> dataImportEventPayloadContext = new HashMap<>() {{
      put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
      put(ERROR_KEY, "java.lang.IllegalStateException: Unsupported Marc record type");
      put("INCOMING_RECORD_ID", incomingRecordId);
    }};

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withJobExecutionId(jobExecution.getId())
      .withContext(dataImportEventPayloadContext)
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withToken("token");

    // when
    Event event = new Event().withEventPayload(Json.encode(dataImportEventPayload))
      .withEventType(DI_ERROR.value())
      .withId(UUID.randomUUID().toString());
    String topic = formatToKafkaTopicName(DI_ERROR.value());
    KeyValue<String, String> kafkaRecord = new KeyValue<>("key", Json.encode(event));
    kafkaRecord.addHeader(OKAPI_TENANT_HEADER, TENANT_ID, UTF_8);
    kafkaRecord.addHeader(OKAPI_URL_HEADER, snapshotMockServer.baseUrl(), UTF_8);
    kafkaRecord.addHeader(JOB_EXECUTION_ID_HEADER, jobExecutionUUID, UTF_8);
    SendKeyValues<String, String> request = SendKeyValues.to(topic, Collections.singletonList(kafkaRecord))
      .useDefaults();
    kafkaCluster.send(request);

    // then
    assertJournalRecord(context, jobExecution.getId(), (journalRecords) -> journalRecords.stream().anyMatch(record -> {
      Assert.assertEquals("Entity Type:", JournalRecord.EntityType.MARC_BIBLIOGRAPHIC.value(), record.getEntityType().value());
      Assert.assertEquals("Action Type:", JournalRecord.ActionType.CREATE.value(), record.getActionType().value());
      Assert.assertEquals("Action Status:", JournalRecord.ActionStatus.ERROR.value(), record.getActionStatus().value());
      Assert.assertEquals("Source Record id:", incomingRecordId, record.getSourceId());
      Assert.assertNotNull(record.getError());
      return true;
    }));
    async.complete();
  }

  @Test
  public void testShouldFillTitleOnRecordModifiedEventProcessing(TestContext context) throws InterruptedException, IOException {
    Async async = context.async();

    // given
    Record record = Json.decodeValue(TestUtil.readFileFromPath(RECORD_PATH), Record.class);
    record.setSnapshotId(jobExecution.getId());
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED.value())
      .withJobExecutionId(jobExecution.getId())
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID)
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
      }});

    // when
    Event event = new Event().withEventPayload(Json.encode(dataImportEventPayload))
      .withEventType(DI_ERROR.value())
      .withId(UUID.randomUUID().toString());
    String topic = formatToKafkaTopicName(DI_SRS_MARC_BIB_RECORD_MODIFIED.value());
    KeyValue<String, String> kafkaRecord = new KeyValue<>("key", Json.encode(event));
    kafkaRecord.addHeader(OKAPI_TENANT_HEADER, TENANT_ID, UTF_8);
    kafkaRecord.addHeader(OKAPI_URL_HEADER, snapshotMockServer.baseUrl(), UTF_8);
    kafkaRecord.addHeader(JOB_EXECUTION_ID_HEADER, jobExecutionUUID, UTF_8);
    SendKeyValues<String, String> request = SendKeyValues.to(topic, Collections.singletonList(kafkaRecord))
      .useDefaults();
    kafkaCluster.send(request);

    // then
    assertJournalRecord(context, jobExecution.getId(), (journalRecords) -> journalRecords.stream().anyMatch(journalRecord -> {
      Assert.assertEquals("Entity Type:", JournalRecord.EntityType.MARC_BIBLIOGRAPHIC, journalRecord.getEntityType());
      Assert.assertEquals("Action Type:", JournalRecord.ActionType.MODIFY, journalRecord.getActionType());
      Assert.assertEquals("Action Status:", JournalRecord.ActionStatus.COMPLETED, journalRecord.getActionStatus());
      Assert.assertEquals("Title:", "The Journal of ecclesiastical history.", journalRecord.getTitle());
      return true;
    }));
    async.complete();
  }

  @Test
  public void shouldNotProcessEventWhenItAlreadyProcessed(TestContext context) throws InterruptedException {
    Async async = context.async();
    // given
    String incomingRecordId = UUID.randomUUID().toString();
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withJobExecutionId(jobExecution.getId())
      // this test will fail if no context is passed. Random UUIDs are assigned to a stub record
      // this break generating a UUID from the properties of the record.
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
        put("INCOMING_RECORD_ID", incomingRecordId);
      }})
      .withOkapiUrl(OKAPI_URL)
      .withTenant(TENANT_ID);

    // when
    Event event = new Event().withEventPayload(Json.encode(dataImportEventPayload))
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withId(UUID.randomUUID().toString());
    String topic = formatToKafkaTopicName(DI_INVENTORY_INSTANCE_CREATED.value());
    KeyValue<String, String> kafkaRecord = new KeyValue<>("key", Json.encode(event));
    kafkaRecord.addHeader(OKAPI_TENANT_HEADER, TENANT_ID, UTF_8);
    kafkaRecord.addHeader(OKAPI_URL_HEADER, snapshotMockServer.baseUrl(), UTF_8);
    kafkaRecord.addHeader(JOB_EXECUTION_ID_HEADER, jobExecutionUUID, UTF_8);
    SendKeyValues<String, String> request = SendKeyValues.to(topic, Collections.singletonList(kafkaRecord))
      .useDefaults();
    kafkaCluster.send(request);
    kafkaCluster.send(request);


    vertx.setTimer(2000, timerId -> {
      // then
      assertJournalRecord(context, jobExecution.getId(), (journalRecords) -> (long) journalRecords.size() == 1);
      async.complete();
    });


  }
}
