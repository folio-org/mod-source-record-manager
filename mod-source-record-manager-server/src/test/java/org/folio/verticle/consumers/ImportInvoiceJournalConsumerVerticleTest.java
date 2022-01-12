package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.folio.DataImportEventPayload;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.dao.JobExecutionDaoImpl;
import org.folio.dao.JournalRecordDao;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionLogDto;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.services.EventProcessedService;
import org.folio.services.journal.JournalService;
import org.folio.verticle.consumers.util.EventTypeHandlerSelector;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.folio.DataImportEventTypes.DI_ERROR;
import static org.folio.DataImportEventTypes.DI_INVOICE_CREATED;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.jaxrs.model.EntityType.EDIFACT_INVOICE;
import static org.folio.rest.jaxrs.model.EntityType.INVOICE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.services.journal.InvoiceUtil.FIELD_DESCRIPTION;
import static org.folio.services.journal.InvoiceUtil.FIELD_FOLIO_INVOICE_NO;
import static org.folio.services.journal.InvoiceUtil.FIELD_ID;
import static org.folio.services.journal.InvoiceUtil.FIELD_INVOICE_LINES;
import static org.folio.services.journal.InvoiceUtil.FIELD_INVOICE_LINE_NUMBER;
import static org.folio.services.journal.InvoiceUtil.FIELD_VENDOR_INVOICE_NO;
import static org.folio.services.journal.InvoiceUtil.INVOICE_LINES_ERRORS_KEY;
import static org.folio.services.journal.InvoiceUtil.INVOICE_LINES_KEY;
import static org.folio.verticle.consumers.ImportInvoiceJournalConsumerVerticleMockTest.DI_POST_INVOICE_LINES_SUCCESS_TENANT;
import static org.folio.verticle.consumers.ImportInvoiceJournalConsumerVerticleMockTest.EDIFACT_PARSED_CONTENT;

@RunWith(VertxUnitRunner.class)
public class ImportInvoiceJournalConsumerVerticleTest extends AbstractRestTest {

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

  private ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfile.getId())
    .withContentType(JOB_PROFILE);

  @Before
  public void setUp() {
    jobExecutionDao = getBeanFromSpringContext(vertx, org.folio.dao.JobExecutionDaoImpl.class);
    Assert.assertNotNull(jobExecutionDao);
    jobExecutionDao.save(jobExecution, TENANT_ID);

    journalRecordDao = getBeanFromSpringContext(vertx, org.folio.dao.JournalRecordDaoImpl.class);
    Assert.assertNotNull(journalRecordDao);

    journalService = getBeanFromSpringContext(vertx, org.folio.services.journal.JournalServiceImpl.class);
    Assert.assertNotNull(journalService);

    eventProcessedService = getBeanFromSpringContext(vertx, EventProcessedService.class);
    Assert.assertNotNull(eventProcessedService);

    EventTypeHandlerSelector eventTypeHandlerSelector = getBeanFromSpringContext(vertx, EventTypeHandlerSelector.class);
    Assert.assertNotNull(eventTypeHandlerSelector);

    dataImportJournalKafkaHandler = new DataImportJournalKafkaHandler(vertx, eventProcessedService, eventTypeHandlerSelector, journalService);
  }

  String INVOICE_ID = UUID.randomUUID().toString();

  JsonObject INVOICE_JSON = new JsonObject()
    .put(FIELD_ID, INVOICE_ID)
    .put(FIELD_FOLIO_INVOICE_NO, "228D126")
    .put(FIELD_VENDOR_INVOICE_NO, "0704159");

  JsonObject INVOICE_LINES_JSON = new JsonObject()
    .put(FIELD_INVOICE_LINES, new JsonArray()
    .add(new JsonObject()
      .put(FIELD_ID, UUID.randomUUID().toString())
      .put(FIELD_DESCRIPTION, "Some description")
      .put(FIELD_INVOICE_LINE_NUMBER, "1")
      .put("invoiceId", INVOICE_ID))
    .add(new JsonObject()
      .put(FIELD_ID, UUID.randomUUID().toString())
      .put(FIELD_DESCRIPTION, "Some description 2")
      .put(FIELD_INVOICE_LINE_NUMBER, "2")
      .put("invoiceId", INVOICE_ID))
    .add(new JsonObject()
      .put(FIELD_ID, UUID.randomUUID().toString())
      .put(FIELD_DESCRIPTION, "Some description 3")
      .put(FIELD_INVOICE_LINE_NUMBER, "3")
      .put("invoiceId", INVOICE_ID)))
    .put("totalRecords", 3);

  @Test
  public void testJournalInventoryInstanceCreatedAction(TestContext context) throws IOException {
    Async async = context.async();

    // given
    HashMap<String, String> payloadContext = new HashMap<>();

    Record record = new Record()
      .withId(UUID.randomUUID().toString())
      .withOrder(0)
      .withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT));
    payloadContext.put(EDIFACT_INVOICE.value(), Json.encode(record));

    payloadContext.put(INVOICE.value(), Json.encode(INVOICE_JSON));
    payloadContext.put(INVOICE_LINES_KEY, Json.encode(INVOICE_LINES_JSON));
    payloadContext.put(INVOICE_LINES_ERRORS_KEY,
      Json.encode(new JsonObject().put("3", "EventProcessingException: Error during invoice lines creation")));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withTenant(DI_POST_INVOICE_LINES_SUCCESS_TENANT)
      .withJobExecutionId(jobExecution.getId())
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withEventsChain(List.of(DI_INVOICE_CREATED.value()));

    // when
    KafkaConsumerRecord<String, String> kafkaConsumerRecord = buildKafkaConsumerRecord(dataImportEventPayload);
    dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    // then
    Future<JobExecutionLogDto> future = journalRecordDao.getJobExecutionLogDto(jobExecution.getId(), TENANT_ID);
    future.onComplete(ar -> {
      if (ar.succeeded()) {
        context.assertTrue(ar.succeeded());
        Assert.assertNotNull(ar.result());
        Assert.assertEquals(Optional.of(4), Optional.ofNullable(ar.result().getJobExecutionResultLogs().get(0).getTotalCompleted()));
      }
    });
    async.complete();
  }

  private KafkaConsumerRecord<String, String> buildKafkaConsumerRecord(DataImportEventPayload record) throws IOException {
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
