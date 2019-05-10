package org.folio.services.afterprocessing;

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
import org.apache.commons.io.FileUtils;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.client.SourceStorageClient;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_TOKEN_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;

@RunWith(VertxUnitRunner.class)
public class AdditionalFieldsUtilTest {

  private static final String TENANT = "diku";
  private static final String TOKEN = "token";
  private static final String SOURCE_STORAGE_SERVICE_URL = "/source-storage/records/";
  private static final String PARSED_RECORD_PATH = "src/test/resources/org/folio/services/afterprocessing/parsedRecord.json";

  @Rule
  public WireMockRule mockServer =
    new WireMockRule(WireMockConfiguration.wireMockConfig().dynamicPort().notifier(new Slf4jNotifier(true)));
  private AdditionalFieldsUtil additionalFieldsUtil = new AdditionalFieldsUtil();
  private Vertx vertx = Vertx.vertx();
  private Map<String, String> headers = new HashMap<>();

  @Before
  public void setUp() {
    headers.put(OKAPI_URL_HEADER, "http://localhost:" + mockServer.port());
    headers.put(OKAPI_TENANT_HEADER, TENANT);
    headers.put(OKAPI_TOKEN_HEADER, TOKEN);
  }

  @Test
  public void shouldAddInstanceIdSubfield(TestContext testContext) throws IOException {
    Async async = testContext.async();
    // given
    String recordId = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();
    RecordProcessingContext recordProcessingContext = new RecordProcessingContext(Record.RecordType.MARC);
    recordProcessingContext.addRecordContext(recordId, instanceId);

    String parsedRecordContent = FileUtils.readFileToString(new File(PARSED_RECORD_PATH), "UTF-8");
    ParsedRecord parsedRecord = new ParsedRecord();
    parsedRecord.setContent(parsedRecordContent);
    Record record = new Record().withId(recordId).withParsedRecord(parsedRecord);
    // when
    Future<Record> future = additionalFieldsUtil.putInstanceIdToMarcRecord(record, recordProcessingContext.getRecordsContext().get(0));
    // then
    future.setHandler(ar -> {
      testContext.assertTrue(ar.succeeded());
      Record actualRecord = ar.result();
      JsonObject content = new JsonObject((String) actualRecord.getParsedRecord().getContent());
      JsonArray fields = content.getJsonArray("fields");
      testContext.assertTrue(!fields.isEmpty());
      for (int i = fields.size(); i-- > 0; ) {
        JsonObject targetField = fields.getJsonObject(i);
        if (targetField.containsKey(AdditionalFieldsConfig.TAG_999)) {
          JsonArray subfields = targetField.getJsonObject(AdditionalFieldsConfig.TAG_999).getJsonArray("subfields");
          for (int j = subfields.size(); j-- > 0; ) {
            JsonObject targetSubfield = subfields.getJsonObject(j);
            if (targetSubfield.containsKey("i")) {
              String actualInstanceId = (String) targetSubfield.getValue("i");
              testContext.assertEquals(instanceId, actualInstanceId);
              async.complete();
            }
          }
        }
      }
    });
  }

  @Test
  public void shouldGetRecordById(TestContext testContext) throws IOException {
    Async async = testContext.async();
    // given
    OkapiConnectionParams params = new OkapiConnectionParams(headers, vertx);
    SourceStorageClient sourceStorageClient = new SourceStorageClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    String recordId = UUID.randomUUID().toString();
    Record record = new Record().withId(recordId);
    WireMock.stubFor(WireMock.get(SOURCE_STORAGE_SERVICE_URL + recordId).willReturn(WireMock.ok(JsonObject.mapFrom(record).toString())));
    // when
    Future<Record> future = additionalFieldsUtil.getRecordById(recordId, sourceStorageClient);
    // then
    future.setHandler(ar -> {
      testContext.assertTrue(ar.succeeded());
      Record actualRecord = ar.result();
      testContext.assertEquals(record.getId(), actualRecord.getId());
      async.complete();
    });
  }
}
