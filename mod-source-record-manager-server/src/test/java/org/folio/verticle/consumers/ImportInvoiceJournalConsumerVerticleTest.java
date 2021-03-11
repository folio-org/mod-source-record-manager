package org.folio.verticle.consumers;

import com.github.tomakehurst.wiremock.admin.NotFoundException;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxImpl;
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
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.MappingProfile;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.dao.JobExecutionDaoImpl;
import org.folio.dao.JournalRecordDao;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionLogDto;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RepeatableSubfieldMapping;
import org.folio.services.journal.JournalService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.folio.ActionProfile.Action.CREATE;
import static org.folio.DataImportEventTypes.DI_INVOICE_CREATED;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.jaxrs.model.EntityType.EDIFACT_INVOICE;
import static org.folio.rest.jaxrs.model.EntityType.INVOICE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.services.journal.InvoiceUtil.FIELD_DESCRIPTION;
import static org.folio.services.journal.InvoiceUtil.FIELD_FOLIO_INVOICE_NO;
import static org.folio.services.journal.InvoiceUtil.FIELD_ID;
import static org.folio.services.journal.InvoiceUtil.FIELD_INVOICE_LINES;
import static org.folio.services.journal.InvoiceUtil.FIELD_INVOICE_LINE_NUMBER;
import static org.folio.services.journal.InvoiceUtil.FIELD_VENDOR_INVOICE_NO;
import static org.folio.services.journal.InvoiceUtil.INVOICE_LINES_KEY;
import static org.folio.verticle.consumers.ImportInvoiceJournalConsumerVerticleMockTest.DI_POST_INVOICE_LINES_SUCCESS_TENANT;
import static org.folio.verticle.consumers.ImportInvoiceJournalConsumerVerticleMockTest.EDIFACT_PARSED_CONTENT;

@RunWith(VertxUnitRunner.class)
public class ImportInvoiceJournalConsumerVerticleTest extends AbstractRestTest {

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

  private ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withAction(CREATE)
    .withFolioRecord(ActionProfile.FolioRecord.INVOICE);

  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withIncomingRecordType(EDIFACT_INVOICE)
    .withExistingRecordType(INVOICE)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(List.of(
        new MappingRule().withPath("invoice.vendorInvoiceNo").withValue("BGM+380+[1]").withEnabled("true"),
        new MappingRule().withPath("invoice.currency").withValue("CUX+2[2]").withEnabled("true"),
        new MappingRule().withPath("invoice.status").withValue("\"Open\"").withEnabled("true"),
        new MappingRule().withPath("invoice.invoiceLines[]").withEnabled("true")
          .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING)
          .withSubfields(List.of(new RepeatableSubfieldMapping()
            .withOrder(0)
            .withPath("invoice.invoiceLines[]")
            .withFields(List.of(
              new MappingRule().withPath("invoice.invoiceLines[].subTotal")
                .withValue("MOA+203[2]"),
              new MappingRule().withPath("invoice.invoiceLines[].quantity")
                .withValue("QTY+47[2]")
            )))))));

  private ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(JsonObject.mapFrom(jobProfile).getMap())
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withId(UUID.randomUUID().toString())
        .withProfileId(actionProfile.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(JsonObject.mapFrom(actionProfile).getMap())
        .withChildSnapshotWrappers(Collections.singletonList(
          new ProfileSnapshotWrapper()
            .withId(UUID.randomUUID().toString())
            .withProfileId(mappingProfile.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(mappingProfile).getMap())))));

  @Before
  public void setUp() {
    jobExecutionDao = getBeanFromSpringContext(vertx, org.folio.dao.JobExecutionDaoImpl.class);
    Assert.assertNotNull(jobExecutionDao);
    jobExecutionDao.save(jobExecution, TENANT_ID);

    journalRecordDao = getBeanFromSpringContext(vertx, org.folio.dao.JournalRecordDaoImpl.class);
    Assert.assertNotNull(journalRecordDao);

    journalService = getBeanFromSpringContext(vertx, org.folio.services.journal.JournalServiceImpl.class);
    Assert.assertNotNull(journalService);

    dataImportJournalKafkaHandler = new DataImportJournalKafkaHandler(vertx, journalService);
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
      .put("invoiceId", INVOICE_ID)))
    .put("totalRecords", 2);

  @Test
  public void testJournalInventoryInstanceCreatedAction(TestContext context) throws IOException {
    Async async = context.async();

    // given
    HashMap<String, String> payloadContext = new HashMap<>();

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT));
    payloadContext.put(EDIFACT_INVOICE.value(), Json.encode(record));

    payloadContext.put(INVOICE.value(), Json.encode(INVOICE_JSON));
    payloadContext.put(INVOICE_LINES_KEY, Json.encode(INVOICE_LINES_JSON));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVOICE_CREATED.value())
      .withTenant(DI_POST_INVOICE_LINES_SUCCESS_TENANT)
      .withJobExecutionId(jobExecution.getId())
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper);

    // when
    KafkaConsumerRecord<String, String> kafkaConsumerRecord = buildKafkaConsumerRecord(dataImportEventPayload);
    dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    // then
    Future<JobExecutionLogDto> future = journalRecordDao.getJobExecutionLogDto(jobExecution.getId(), TENANT_ID);
    future.onComplete(ar -> {
      if (ar.succeeded()) {
        context.assertTrue(ar.succeeded());
        Assert.assertNotNull(ar.result());
        Assert.assertEquals(Optional.of(3), Optional.ofNullable(ar.result().getJobExecutionResultLogs().get(0).getTotalCompleted()));
      }
    });
    async.complete();
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

  private <T> T getBeanFromSpringContext(Vertx vtx, Class<T> clazz) {

    String parentVerticleUUID = vertx.deploymentIDs().stream()
      .filter(v -> !((VertxImpl) vertx).getDeployment(v).isChild())
      .findFirst()
      .orElseThrow(() -> new NotFoundException("Couldn't find the parent verticle."));

    Optional<Object> context = Optional.of(((VertxImpl) vtx).getDeployment(parentVerticleUUID).getContexts().stream()
      .findFirst().map(v -> v.get("springContext")))
      .orElseThrow(() -> new NotFoundException("Couldn't find the spring context."));

    if (context.isPresent()) {
      return ((AnnotationConfigApplicationContext) context.get()).getBean(clazz);
    }
    throw new NotFoundException(String.format("Couldn't find bean %s", clazz.getName()));
  }

}
