package org.folio.verticle.consumers;

import io.vertx.core.Future;
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
import org.folio.dao.JobExecutionDaoImpl;
import org.folio.dao.JournalRecordDao;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.services.EventProcessedService;
import org.folio.services.EventProcessedServiceImpl;
import org.folio.services.journal.JournalService;
import org.folio.verticle.consumers.util.EventTypeHandlerSelector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_LOG_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDING_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;

@RunWith(VertxUnitRunner.class)
public class DataImportJournalConsumerVerticleTest extends AbstractRestTest {

  private EventProcessedService eventProcessedService;
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

    eventProcessedService = getBeanFromSpringContext(vertx, EventProcessedServiceImpl.class);
    Assert.assertNotNull(eventProcessedService);

    EventTypeHandlerSelector eventTypeHandlerSelector = getBeanFromSpringContext(vertx, EventTypeHandlerSelector.class);
    Assert.assertNotNull(eventTypeHandlerSelector);

    dataImportJournalKafkaHandler = new DataImportJournalKafkaHandler(vertx, eventProcessedService, eventTypeHandlerSelector, journalService);
  }

  @Test
  public void testJournalRecordMappingError(TestContext context) throws IOException, ExecutionException, InterruptedException {
    Async async = context.async();

    // given
    String topic = KafkaTopicNameHelper.formatTopicName("folio", getDefaultNameSpace(), TENANT_ID, DI_SRS_MARC_HOLDING_RECORD_CREATED.value());
    Event event = new Event().withEventPayload(null).withEventType(DI_LOG_SRS_MARC_BIB_RECORD_CREATED.value()).withId(UUID.randomUUID().toString());
    ConsumerRecord<String, String> consumerRecord = buildConsumerRecord(topic, event);

    // when
    Future<String> future = dataImportJournalKafkaHandler.handle(new KafkaConsumerRecordImpl<>(consumerRecord));

    // then
    future.onComplete(ar -> {
        context.assertTrue(ar.failed());
        async.complete();
      });
  }

  private KafkaConsumerRecord<String, String> buildKafkaConsumerRecord(DataImportEventPayload record) {
    String topic = KafkaTopicNameHelper.formatTopicName("folio", getDefaultNameSpace(), TENANT_ID, record.getEventType());
    Event event = new Event().withEventPayload(Json.encode(record));
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
