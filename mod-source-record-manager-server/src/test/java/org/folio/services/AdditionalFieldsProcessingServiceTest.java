package org.folio.services;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.afterProcessing.AdditionalFieldsConfig;
import org.folio.services.afterProcessing.AdditionalFieldsProcessingServiceImpl;
import org.folio.services.afterProcessing.AfterProcessingService;
import org.folio.services.afterProcessing.RecordProcessingContext;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_TOKEN_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;

@RunWith(VertxUnitRunner.class)
public class AdditionalFieldsProcessingServiceTest {

  private static final String TENANT = "diku";
  private static final String TOKEN = "token";
  private static final String SOURCE_STORAGE_SERVICE_URL = "/source-storage/records/";
  private final String SOURCE_CHUNK_ID = null;
  @Rule
  public WireMockRule mockServer =
    new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort().notifier(new Slf4jNotifier(true)));
  private AdditionalFieldsConfig additionalFieldsConfig = Mockito.spy(AdditionalFieldsConfig.class);
  private AfterProcessingService additionalFieldsProcessingService =
    new AdditionalFieldsProcessingServiceImpl(additionalFieldsConfig);
  private Vertx vertx = Vertx.vertx();
  private Map<String, String> headers = new HashMap<>();
  private JsonObject PARSED_RECORD = new JsonObject().put("fields", new JsonArray());

  @Before
  public void setUp() {
    headers.put(OKAPI_URL_HEADER, "http://localhost:" + mockServer.port());
    headers.put(OKAPI_TENANT_HEADER, TENANT);
    headers.put(OKAPI_TOKEN_HEADER, TOKEN);
    mockSourceRecordService();
  }

  @Test
  public void shouldReturnEmptyContextIfEmptyContextReceived(TestContext testContext) {
    Async async = testContext.async();
    RecordProcessingContext givenProcessingContext = new RecordProcessingContext(Collections.EMPTY_LIST);

    Future<RecordProcessingContext> future = additionalFieldsProcessingService.process(givenProcessingContext, null, null);

    future.setHandler(ar -> {
      testContext.assertTrue(ar.succeeded());
      RecordProcessingContext actualProcessingContext = ar.result();
      testContext.assertTrue(actualProcessingContext.getRecordsContext().isEmpty());
      async.complete();
    });
  }

  @Test
  public void shouldNotCreateAdditionalFieldsForNonMarcParsedRecords(TestContext testContext) {
    Async async = testContext.async();
    Record.RecordType recordType = null;

    Future<RecordProcessingContext> future = additionalFieldsProcessingService
      .process(getRecordProcessingContext(recordType), SOURCE_CHUNK_ID, new OkapiConnectionParams(headers, vertx));

    future.setHandler(ar -> {
      testContext.assertTrue(ar.succeeded());
      RecordProcessingContext processingContext = ar.result();
      JsonObject content =
        new JsonObject((String) processingContext.getRecordsContext().get(0).getRecord().getParsedRecord().getContent());
      JsonArray fields = content.getJsonArray("fields");
      testContext.assertTrue(fields.isEmpty());
      async.complete();
    });
  }

  @Test
  public void shouldCreateAdditionalFieldsForMarcParsedRecords(TestContext testContext) {
    Async async = testContext.async();
    Record.RecordType recordType = Record.RecordType.MARC;

    Future<RecordProcessingContext> future = additionalFieldsProcessingService
      .process(getRecordProcessingContext(recordType), SOURCE_CHUNK_ID, new OkapiConnectionParams(headers, vertx));

    future.setHandler(ar -> {
      testContext.assertTrue(ar.succeeded());
      RecordProcessingContext processingContext = ar.result();
      JsonObject content =
        new JsonObject((String) processingContext.getRecordsContext().get(0).getRecord().getParsedRecord().getContent());
      JsonArray fields = content.getJsonArray("fields");
      testContext.assertFalse(fields.isEmpty());
      JsonObject additionalField = fields.getJsonObject(0);
      testContext.assertTrue(additionalField.containsKey(AdditionalFieldsConfig.TAG_999));
      async.complete();
    });
  }

  private RecordProcessingContext getRecordProcessingContext(Record.RecordType type) {
    Record record = new Record();
    record.setId(UUID.randomUUID().toString());
    record.setRecordType(type);
    record.setParsedRecord(new ParsedRecord().withContent(PARSED_RECORD.toString()));
    RecordProcessingContext context = new RecordProcessingContext(Collections.singletonList(record));
    context.getRecordsContext().get(0).setInstanceId(UUID.randomUUID().toString());
    return context;
  }

  private void mockSourceRecordService() {
    WireMock.stubFor(WireMock.post(SOURCE_STORAGE_SERVICE_URL)
      .willReturn(WireMock.serverError()));
  }
}
