package org.folio.verticle.consumers.errorhandlers.errorpayloadbuilders;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.folio.DataImportEventPayload;
import org.folio.TestUtil;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.verticle.consumers.errorhandlers.payloadbuilders.MarcHoldingsErrorPayloadBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.verticle.consumers.errorhandlers.RawMarcChunksErrorHandler.ERROR_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(VertxUnitRunner.class)
public class MarcHoldingsErrorPayloadBuilderTest {

  private static final String TENANT_ID = "diku";
  private static final String TOKEN = "token";
  private static final String JOB_EXECUTION_ID = UUID.randomUUID().toString();
  private static final String PARSED_RECORD_PATH = "src/test/resources/org/folio/services/afterprocessing/parsedRecord.json";
  private static final String LARGE_PAYLOAD_ERROR_MESSAGE = "Record size is greater that MAX_REQUEST_SIZE";

  @Mock
  private Vertx vertx;

  @InjectMocks
  private MarcHoldingsErrorPayloadBuilder payloadBuilder = new MarcHoldingsErrorPayloadBuilder();

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void checkEligible() {
    boolean eligible = payloadBuilder.isEligible(Record.RecordType.MARC_HOLDING);
    assertTrue(eligible);
  }

  @Test
  public void checkNotEligible() {
    boolean eligible = payloadBuilder.isEligible(Record.RecordType.EDIFACT);
    assertFalse(eligible);
  }

  @Test
  public void shouldBuildPayload(TestContext context) throws IOException {
    Async async = context.async();
    Record record = getRecordFromFile();

    Future<DataImportEventPayload> payloadFuture = payloadBuilder.buildEventPayload(new RecordTooLargeException(LARGE_PAYLOAD_ERROR_MESSAGE),
      getOkapiParams(), JOB_EXECUTION_ID, record);

    payloadFuture.onComplete(ar -> {
      DataImportEventPayload result = ar.result();
      assertEquals(DI_ERROR.value(), result.getEventType());
      assertTrue(result.getContext().containsKey(ERROR_KEY));
      async.complete();
    });
  }

  @Test
  public void shouldBuildPayloadWhenTitleNotExistsInParsedRecord(TestContext context) throws IOException {
    Async async = context.async();
    Record record = new Record().withRecordType(Record.RecordType.MARC_HOLDING).withParsedRecord(
      new ParsedRecord().withId(UUID.randomUUID().toString()).withContent("{\"leader\":\"01240cas a2200397   4500\",\"fields\":[]}"));

    Future<DataImportEventPayload> payloadFuture = payloadBuilder.buildEventPayload(new RecordTooLargeException(LARGE_PAYLOAD_ERROR_MESSAGE),
      getOkapiParams(), JOB_EXECUTION_ID, record);

    payloadFuture.onComplete(ar -> {
      DataImportEventPayload result = ar.result();
      assertEquals(DI_ERROR.value(), result.getEventType());
      assertTrue(result.getContext().containsKey(ERROR_KEY));

      Record resRecordWithNoTitle = getRecordFromContext(result);
      assertNull(resRecordWithNoTitle.getParsedRecord());
      async.complete();
    });
  }

  private OkapiConnectionParams getOkapiParams() {
    HashMap<String, String> headers = new HashMap<>();
    headers.put(OKAPI_URL_HEADER, "http://localhost");
    headers.put(OKAPI_TENANT_HEADER, TENANT_ID);
    headers.put(OKAPI_TOKEN_HEADER, TOKEN);
    return new OkapiConnectionParams(headers, vertx);
  }

  private Record getRecordFromFile() throws IOException {
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_RECORD_PATH);
    return new Record()
      .withRecordType(Record.RecordType.MARC_HOLDING)
      .withParsedRecord(new ParsedRecord().withContent(parsedRecordContent));
  }

  private Record getRecordFromContext(DataImportEventPayload eventPayload) {
    String recordStr = eventPayload.getContext().get(EntityType.MARC_HOLDINGS.value());
    return Json.decodeValue(recordStr, Record.class);
  }
}
