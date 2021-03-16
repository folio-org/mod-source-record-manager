package org.folio.verticle.consumers;

import io.vertx.core.Vertx;
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
import org.folio.dao.JournalRecordDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.JournalRecord.ActionStatus;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.services.journal.JournalRecordMapperException;
import org.folio.services.journal.JournalServiceImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.folio.ActionProfile.Action.CREATE;
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
import static org.folio.services.journal.JournalUtil.ERROR_KEY;

@RunWith(VertxUnitRunner.class)
public class ImportInvoiceJournalConsumerVerticleMockTest extends AbstractRestTest {

  static final String EDIFACT_PARSED_CONTENT = "{\"segments\": [{\"tag\": \"UNA\", \"dataElements\": []}, {\"tag\": \"UNB\", \"dataElements\": [{\"components\": [{\"data\": \"UNOC\"}, {\"data\": \"3\"}]}, {\"components\": [{\"data\": \"EBSCO\"}, {\"data\": \"92\"}]}, {\"components\": [{\"data\": \"KOH0002\"}, {\"data\": \"91\"}]}, {\"components\": [{\"data\": \"200610\"}, {\"data\": \"0105\"}]}, {\"components\": [{\"data\": \"5162\"}]}]}, {\"tag\": \"UNH\", \"dataElements\": [{\"components\": [{\"data\": \"5162\"}]}, {\"components\": [{\"data\": \"INVOIC\"}, {\"data\": \"D\"}, {\"data\": \"96A\"}, {\"data\": \"UN\"}, {\"data\": \"EAN008\"}]}]}, {\"tag\": \"BGM\", \"dataElements\": [{\"components\": [{\"data\": \"380\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"JINV\"}]}, {\"components\": [{\"data\": \"0704159\"}]}, {\"components\": [{\"data\": \"43\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"137\"}, {\"data\": \"20191002\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"NAD\", \"dataElements\": [{\"components\": [{\"data\": \"BY\"}]}, {\"components\": [{\"data\": \"BR1624506\"}, {\"data\": \"\"}, {\"data\": \"91\"}]}]}, {\"tag\": \"NAD\", \"dataElements\": [{\"components\": [{\"data\": \"SR\"}]}, {\"components\": [{\"data\": \"EBSCO\"}, {\"data\": \"\"}, {\"data\": \"92\"}]}]}, {\"tag\": \"CUX\", \"dataElements\": [{\"components\": [{\"data\": \"2\"}, {\"data\": \"USD\"}, {\"data\": \"4\"}]}]}, {\"tag\": \"LIN\", \"dataElements\": [{\"components\": [{\"data\": \"1\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5\"}]}, {\"components\": [{\"data\": \"004362033\"}, {\"data\": \"SA\"}]}, {\"components\": [{\"data\": \"1941-6067\"}, {\"data\": \"IS\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5S\"}]}, {\"components\": [{\"data\": \"1941-6067(20200101)14;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5E\"}]}, {\"components\": [{\"data\": \"1941-6067(20201231)14;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"IMD\", \"dataElements\": [{\"components\": [{\"data\": \"L\"}]}, {\"components\": [{\"data\": \"050\"}]}, {\"components\": [{\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"ACADEMY OF MANAGEMENT ANNALS -   ON\"}, {\"data\": \"LINE FOR INSTITUTIONS\"}]}]}, {\"tag\": \"QTY\", \"dataElements\": [{\"components\": [{\"data\": \"47\"}, {\"data\": \"1\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"194\"}, {\"data\": \"20200101\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"206\"}, {\"data\": \"20201231\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"203\"}, {\"data\": \"208.59\"}, {\"data\": \"USD\"}, {\"data\": \"4\"}]}]}, {\"tag\": \"PRI\", \"dataElements\": [{\"components\": [{\"data\": \"AAB\"}, {\"data\": \"205\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"LI\"}, {\"data\": \"S255699\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"SNA\"}, {\"data\": \"C6546362\"}]}]}, {\"tag\": \"ALC\", \"dataElements\": [{\"components\": [{\"data\": \"C\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"G74\"}, {\"data\": \"\"}, {\"data\": \"28\"}, {\"data\": \"LINE SERVICE CHARGE\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"8\"}, {\"data\": \"3.59\"}]}]}, {\"tag\": \"LIN\", \"dataElements\": [{\"components\": [{\"data\": \"2\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5\"}]}, {\"components\": [{\"data\": \"006288237\"}, {\"data\": \"SA\"}]}, {\"components\": [{\"data\": \"1944-737X\"}, {\"data\": \"IS\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5S\"}]}, {\"components\": [{\"data\": \"1944-737X(20200301)117;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5E\"}]}, {\"components\": [{\"data\": \"1944-737X(20210228)118;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"IMD\", \"dataElements\": [{\"components\": [{\"data\": \"L\"}]}, {\"components\": [{\"data\": \"050\"}]}, {\"components\": [{\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"ACI MATERIALS JOURNAL - ONLINE   -\"}, {\"data\": \"MULTI USER\"}]}]}, {\"tag\": \"QTY\", \"dataElements\": [{\"components\": [{\"data\": \"47\"}, {\"data\": \"1\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"194\"}, {\"data\": \"20200301\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"206\"}, {\"data\": \"20210228\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"203\"}, {\"data\": \"726.5\"}, {\"data\": \"USD\"}, {\"data\": \"4\"}]}]}, {\"tag\": \"PRI\", \"dataElements\": [{\"components\": [{\"data\": \"AAB\"}, {\"data\": \"714\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"LI\"}, {\"data\": \"S283902\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"SNA\"}, {\"data\": \"E9498295\"}]}]}, {\"tag\": \"ALC\", \"dataElements\": [{\"components\": [{\"data\": \"C\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"G74\"}, {\"data\": \"\"}, {\"data\": \"28\"}, {\"data\": \"LINE SERVICE CHARGE\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"8\"}, {\"data\": \"12.5\"}]}]}, {\"tag\": \"LIN\", \"dataElements\": [{\"components\": [{\"data\": \"3\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5\"}]}, {\"components\": [{\"data\": \"006289532\"}, {\"data\": \"SA\"}]}, {\"components\": [{\"data\": \"1944-7361\"}, {\"data\": \"IS\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5S\"}]}, {\"components\": [{\"data\": \"1944-7361(20200301)117;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"PIA\", \"dataElements\": [{\"components\": [{\"data\": \"5E\"}]}, {\"components\": [{\"data\": \"1944-7361(20210228)118;1-F\"}, {\"data\": \"SI\"}, {\"data\": \"\"}, {\"data\": \"28\"}]}]}, {\"tag\": \"IMD\", \"dataElements\": [{\"components\": [{\"data\": \"L\"}]}, {\"components\": [{\"data\": \"050\"}]}, {\"components\": [{\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"GRADUATE PROGRAMS IN PHYSICS, ASTRO\"}, {\"data\": \"NOMY AND \"}]}]}, {\"tag\": \"IMD\", \"dataElements\": [{\"components\": [{\"data\": \"L\"}]}, {\"components\": [{\"data\": \"050\"}]}, {\"components\": [{\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"\"}, {\"data\": \"RELATED FIELDS.\"}]}]}, {\"tag\": \"QTY\", \"dataElements\": [{\"components\": [{\"data\": \"47\"}, {\"data\": \"1\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"194\"}, {\"data\": \"20200301\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"DTM\", \"dataElements\": [{\"components\": [{\"data\": \"206\"}, {\"data\": \"20210228\"}, {\"data\": \"102\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"203\"}, {\"data\": \"726.5\"}, {\"data\": \"USD\"}, {\"data\": \"4\"}]}]}, {\"tag\": \"PRI\", \"dataElements\": [{\"components\": [{\"data\": \"AAB\"}, {\"data\": \"714\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"LI\"}, {\"data\": \"S283901\"}]}]}, {\"tag\": \"RFF\", \"dataElements\": [{\"components\": [{\"data\": \"SNA\"}, {\"data\": \"E9498296\"}]}]}, {\"tag\": \"ALC\", \"dataElements\": [{\"components\": [{\"data\": \"C\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"G74\"}, {\"data\": \"\"}, {\"data\": \"28\"}, {\"data\": \"LINE SERVICE CHARGE\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"8\"}, {\"data\": \"12.5\"}]}]}, {\"tag\": \"UNS\", \"dataElements\": [{\"components\": [{\"data\": \"S\"}]}]}, {\"tag\": \"CNT\", \"dataElements\": [{\"components\": [{\"data\": \"1\"}, {\"data\": \"3\"}]}]}, {\"tag\": \"CNT\", \"dataElements\": [{\"components\": [{\"data\": \"2\"}, {\"data\": \"3\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"79\"}, {\"data\": \"18929.07\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"9\"}, {\"data\": \"18929.07\"}]}]}, {\"tag\": \"ALC\", \"dataElements\": [{\"components\": [{\"data\": \"C\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"\"}]}, {\"components\": [{\"data\": \"G74\"}, {\"data\": \"\"}, {\"data\": \"28\"}, {\"data\": \"TOTAL SERVICE CHARGE\"}]}]}, {\"tag\": \"MOA\", \"dataElements\": [{\"components\": [{\"data\": \"8\"}, {\"data\": \"325.59\"}]}]}, {\"tag\": \"UNT\", \"dataElements\": [{\"components\": [{\"data\": \"294\"}]}, {\"components\": [{\"data\": \"5162-1\"}]}]}, {\"tag\": \"UNZ\", \"dataElements\": [{\"components\": [{\"data\": \"1\"}]}, {\"components\": [{\"data\": \"5162\"}]}]}]}";

  public static final String DI_POST_INVOICE_LINES_SUCCESS_TENANT = "di_post_invoice_invoice_lines_success_tenant";
  public static final String ENV_KEY = "folio";
  public static final String ERROR = "error";
  public static final String ACTION_STATUS = "actionStatus";

  @Spy
  private Vertx vertx = Vertx.vertx();

  @InjectMocks
  private JournalRecordDaoImpl journalRecordDao;

  @Spy
  private PostgresClientFactory postgresClientFactory = new PostgresClientFactory(Vertx.vertx());

  @Spy
  private JournalServiceImpl journalService = new JournalServiceImpl(journalRecordDao);

  @Captor
  private ArgumentCaptor<JsonArray> invoiceRecordCaptor;

  private DataImportJournalKafkaHandler dataImportJournalKafkaHandler;

  private ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withAction(CREATE)
    .withFolioRecord(ActionProfile.FolioRecord.INVOICE);

  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withIncomingRecordType(EDIFACT_INVOICE)
    .withExistingRecordType(INVOICE);

  private ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfile.getId())
    .withContentType(JOB_PROFILE);

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

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    dataImportJournalKafkaHandler = new DataImportJournalKafkaHandler(vertx, journalService);
  }

  @Test
  public void shouldProcessEvent(TestContext context) throws IOException, JournalRecordMapperException {
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
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper);

    Mockito.doNothing().when(journalService).saveBatch(ArgumentMatchers.any(JsonArray.class), ArgumentMatchers.any(String.class));

    KafkaConsumerRecord<String, String> kafkaConsumerRecord = buildKafkaConsumerRecord(dataImportEventPayload);
    dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    Mockito.verify(journalService).saveBatch(invoiceRecordCaptor.capture(), Mockito.anyString());

    JsonArray jsonArray = invoiceRecordCaptor.getValue();
    Assert.assertEquals(3, jsonArray.size());
    Assert.assertEquals("Invoice title:", "Invoice", jsonArray.getJsonObject(0).getString("title"));
    Assert.assertEquals("Invoice line 1 -> title:", "Some description", jsonArray.getJsonObject(1).getString("title"));
    Assert.assertEquals("Invoice line 2 -> title:", "Some description 2", jsonArray.getJsonObject(2).getString("title"));

    async.complete();
  }

  @Test
  public void shouldProcessInvoiceErrorEvent(TestContext context) throws IOException, JournalRecordMapperException {
    Async async = context.async();

    // given
    HashMap<String, String> payloadContext = new HashMap<>();

    JsonObject INVOICE_LINES_JSON = new JsonObject()
      .put(FIELD_INVOICE_LINES, new JsonArray()
        .add(new JsonObject()
          .put(FIELD_ID, UUID.randomUUID().toString())
          .put(FIELD_DESCRIPTION, "Some description")
          .put(FIELD_INVOICE_LINE_NUMBER, "")
          .put("invoiceId", INVOICE_ID))
        .add(new JsonObject()
          .put(FIELD_ID, UUID.randomUUID().toString())
          .put(FIELD_DESCRIPTION, "Some description 2")
          .put(FIELD_INVOICE_LINE_NUMBER, null)
          .put("invoiceId", INVOICE_ID)))
      .put("totalRecords", 2);

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT));
    payloadContext.put(EDIFACT_INVOICE.value(), Json.encode(record));

    payloadContext.put(INVOICE.value(), Json.encode(INVOICE_JSON));
    payloadContext.put(INVOICE_LINES_KEY, Json.encode(INVOICE_LINES_JSON));
    payloadContext.put(ERROR_KEY, "Some error in invoice.");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withTenant(DI_POST_INVOICE_LINES_SUCCESS_TENANT)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withEventsChain(List.of(DI_INVOICE_CREATED.value()));

    Mockito.doNothing().when(journalService).saveBatch(ArgumentMatchers.any(JsonArray.class), ArgumentMatchers.any(String.class));

    KafkaConsumerRecord<String, String> kafkaConsumerRecord = buildKafkaConsumerRecord(dataImportEventPayload);
    dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    Mockito.verify(journalService).saveBatch(invoiceRecordCaptor.capture(), Mockito.anyString());

    JsonArray jsonArray = invoiceRecordCaptor.getValue();
    Assert.assertEquals(3, jsonArray.size());
    Assert.assertTrue(jsonArray.getJsonObject(0).getString(ERROR).length() > 0);
    Assert.assertEquals("Invoice: ", ActionStatus.ERROR.value(), jsonArray.getJsonObject(0).getString(ACTION_STATUS));
    Assert.assertTrue(jsonArray.getJsonObject(1).getString(ERROR).length() > 0);
    Assert.assertEquals("Invoice line 1:", ActionStatus.ERROR.value(), jsonArray.getJsonObject(1).getString(ACTION_STATUS));
    Assert.assertTrue(jsonArray.getJsonObject(2).getString(ERROR).length() > 0);
    Assert.assertEquals("Invoice line 2:", ActionStatus.ERROR.value(), jsonArray.getJsonObject(2).getString(ACTION_STATUS));

    async.complete();
  }

  @Test
  public void shouldProcessInvoiceLineErrorEvent(TestContext context) throws IOException, JournalRecordMapperException {
    Async async = context.async();

    // given
    HashMap<String, String> payloadContext = new HashMap<>();

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT));
    payloadContext.put(EDIFACT_INVOICE.value(), Json.encode(record));

    payloadContext.put(INVOICE.value(), Json.encode(INVOICE_JSON));
    payloadContext.put(INVOICE_LINES_KEY, Json.encode(INVOICE_LINES_JSON));
    payloadContext.put(INVOICE_LINES_ERRORS_KEY,
      Json.encode(new JsonObject().put("2", "EventProcessingException: Error during invoice lines creation")));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withTenant(DI_POST_INVOICE_LINES_SUCCESS_TENANT)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withEventsChain(List.of(DI_INVOICE_CREATED.value()));

    Mockito.doNothing().when(journalService).saveBatch(ArgumentMatchers.any(JsonArray.class), ArgumentMatchers.any(String.class));

    KafkaConsumerRecord<String, String> kafkaConsumerRecord = buildKafkaConsumerRecord(dataImportEventPayload);
    dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    Mockito.verify(journalService).saveBatch(invoiceRecordCaptor.capture(), Mockito.anyString());

    JsonArray jsonArray = invoiceRecordCaptor.getValue();
    Assert.assertEquals(3, jsonArray.size());
    Assert.assertTrue(jsonArray.getJsonObject(2).getString(ERROR).length() > 0);
    Assert.assertEquals("Invoice line 2:", ActionStatus.ERROR.value(), jsonArray.getJsonObject(2).getString(ACTION_STATUS));

    async.complete();
  }

  @Test
  public void shouldProcessInvoiceErrorWithoutInvoiceLineOrderEvent(TestContext context) throws IOException {
    Async async = context.async();

    // given
    HashMap<String, String> payloadContext = new HashMap<>();

    Record record = new Record().withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(EDIFACT_PARSED_CONTENT));
    payloadContext.put(EDIFACT_INVOICE.value(), Json.encode(record));

    payloadContext.put(INVOICE.value(), Json.encode(INVOICE_JSON));
    payloadContext.put(INVOICE_LINES_KEY, Json.encode(INVOICE_LINES_JSON));
    payloadContext.put(INVOICE_LINES_ERRORS_KEY,
      Json.encode(new JsonObject().put("2", "EventProcessingException: Error during invoice lines creation")));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withTenant(DI_POST_INVOICE_LINES_SUCCESS_TENANT)
      .withOkapiUrl(OKAPI_URL)
      .withToken(TOKEN)
      .withContext(payloadContext)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withEventsChain(List.of(DI_INVOICE_CREATED.value()));

    Mockito.doNothing().when(journalService).saveBatch(ArgumentMatchers.any(JsonArray.class), ArgumentMatchers.any(String.class));

    KafkaConsumerRecord<String, String> kafkaConsumerRecord = buildKafkaConsumerRecord(dataImportEventPayload);
    dataImportJournalKafkaHandler.handle(kafkaConsumerRecord);

    Mockito.verify(journalService).saveBatch(invoiceRecordCaptor.capture(), Mockito.anyString());

    JsonArray jsonArray = invoiceRecordCaptor.getValue();
    Assert.assertEquals(3, jsonArray.size());
    Assert.assertTrue(jsonArray.getJsonObject(2).getString(ERROR).length() > 0);
    Assert.assertEquals("Invoice line 2:", ActionStatus.ERROR.value(), jsonArray.getJsonObject(2).getString(ACTION_STATUS));

    async.complete();
  }

  private KafkaConsumerRecord<String, String> buildKafkaConsumerRecord(DataImportEventPayload record) throws IOException {
    String topic = KafkaTopicNameHelper.formatTopicName(ENV_KEY, getDefaultNameSpace(), TENANT_ID, record.getEventType());
    Event event = new Event().withEventPayload(ZIPArchiver.zip(Json.encode(record)));
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
