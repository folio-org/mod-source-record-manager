package org.folio.verticle.consumers;

import com.github.tomakehurst.wiremock.admin.NotFoundException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxImpl;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.dao.JobExecutionDaoImpl;
import org.folio.dao.JournalRecordDao;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionLogDto;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.services.journal.JournalService;
import org.folio.verticle.consumers.util.EventTypeHandlerSelector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_COMPLETED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDING_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_ITEM_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_UPDATED;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;

@RunWith(VertxUnitRunner.class)
public class DataImportJournalConsumerVerticleTest extends AbstractRestTest {

  private KafkaInternalCache kafkaInternalCache;
  private JournalService journalService;
  private JobExecutionDaoImpl jobExecutionDao;
  private JournalRecordDao journalRecordDao;
  private DataImportJournalKafkaHandler dataImportJournalKafkaHandler;

  private String jobExecutionUUID = "5105b55a-b9a3-4f76-9402-a5243ea63c95";

  private JobExecution jobExecution = new JobExecution()
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

  private final HashMap<String, String> dataImportEventPayloadContext = new HashMap<>() {{
    put(INSTANCE.value(), recordJson.encode());
    put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
  }};

  @Before
  public void setUp() {
    jobExecutionDao = getBeanFromSpringContext(vertx, org.folio.dao.JobExecutionDaoImpl.class);
    Assert.assertNotNull(jobExecutionDao);
    jobExecutionDao.save(jobExecution, TENANT_ID);

    journalRecordDao = getBeanFromSpringContext(vertx, org.folio.dao.JournalRecordDaoImpl.class);
    Assert.assertNotNull(journalRecordDao);

    journalService = getBeanFromSpringContext(vertx, org.folio.services.journal.JournalServiceImpl.class);
    Assert.assertNotNull(journalService);

    kafkaInternalCache = getBeanFromSpringContext(vertx, KafkaInternalCache.class);
    Assert.assertNotNull(kafkaInternalCache);

    EventTypeHandlerSelector eventTypeHandlerSelector = getBeanFromSpringContext(vertx, EventTypeHandlerSelector.class);
    Assert.assertNotNull(eventTypeHandlerSelector);

    dataImportJournalKafkaHandler = new DataImportJournalKafkaHandler(vertx, kafkaInternalCache, eventTypeHandlerSelector, journalService);
  }

  @Test
  public void testJournalInventoryInstanceCreatedAction(TestContext context) throws IOException {
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
    KafkaConsumerRecord<String, String> kafkaConsumerRecord = buildKafkaConsumerRecord(dataImportEventPayload);
    dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    // then
    Future<JobExecutionLogDto> future = journalRecordDao.getJobExecutionLogDto(jobExecution.getId(), TENANT_ID);
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      Assert.assertNotNull(ar.result());
      async.complete();
    });
  }

  @Test
  public void testJournalMarcBibRecordUpdatedAction(TestContext context) throws IOException {
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
    KafkaConsumerRecord<String, String> kafkaConsumerRecord = buildKafkaConsumerRecord(dataImportEventPayload);
    dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    // then
    Future<JobExecutionLogDto> future = journalRecordDao.getJobExecutionLogDto(jobExecution.getId(), TENANT_ID);
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      Assert.assertNotNull(ar.result());
      async.complete();
    });
  }

  @Test
  public void testJournalCompletedAction(TestContext context) throws IOException, ExecutionException, InterruptedException {
    Async async = context.async();

    // given
    DataImportEventPayload completedEventPayload = new DataImportEventPayload()
      .withEventType(DI_COMPLETED.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(new HashMap<>() {{
        put(ITEM.value(), recordJson.encode());
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
    KafkaConsumerRecord<String, String> kafkaConsumerRecord = buildKafkaConsumerRecord(completedEventPayload);
    dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    // then
    Future<JobExecutionLogDto> future = journalRecordDao.getJobExecutionLogDto(jobExecution.getId(), TENANT_ID);
    future.onComplete(ar -> {
      if (ar.succeeded()) {
        context.assertTrue(ar.succeeded());
        Assert.assertNotNull(ar.result());
      }
    });
    async.complete();
  }

  @Test
  public void testJournalErrorAction(TestContext context) throws IOException, ExecutionException, InterruptedException {
    Async async = context.async();

    // given
    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withTenant(TENANT_ID)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
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
      .withEventsChain(List.of(DI_SRS_MARC_BIB_RECORD_CREATED.value(), DI_INVENTORY_HOLDING_CREATED.value()));

    // when
    KafkaConsumerRecord<String, String> kafkaConsumerRecord = buildKafkaConsumerRecord(eventPayload);
    dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    // then
    Future<JobExecutionLogDto> future = journalRecordDao.getJobExecutionLogDto(jobExecution.getId(), TENANT_ID);
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      Assert.assertNotNull(ar.result());
      async.complete();
    });
  }

  private KafkaConsumerRecord<String, String> buildKafkaConsumerRecord(DataImportEventPayload record) throws IOException {
    String topic = KafkaTopicNameHelper.formatTopicName("folio", getDefaultNameSpace(), TENANT_ID, record.getEventType());
    Event event = new Event().withEventPayload(ZIPArchiver.zip(Json.encode(record)));
    ConsumerRecord<String, String> consumerRecord = buildConsumerRecord(topic, event);
    return new KafkaConsumerRecordImpl<>(consumerRecord);
  }

  private ConsumerRecord<String, String> buildConsumerRecord(String topic, Event event) {
    ConsumerRecord<java.lang.String, java.lang.String> consumerRecord = new ConsumerRecord("folio", 0, 0, topic, Json.encode(event));
    consumerRecord.headers().add(new RecordHeader(OKAPI_TENANT_HEADER, TENANT_ID.getBytes(StandardCharsets.UTF_8)));
    consumerRecord.headers().add(new RecordHeader(OKAPI_URL_HEADER, ("http://localhost:" + snapshotMockServer.port()).getBytes(StandardCharsets.UTF_8)));
    consumerRecord.headers().add(new RecordHeader(OKAPI_TOKEN_HEADER, (TOKEN).getBytes(StandardCharsets.UTF_8)));
    return consumerRecord;
  }

}
