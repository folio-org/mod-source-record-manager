package org.folio.verticle.consumers.errorhandlers.errorpayloadbuilders;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.folio.DataImportEventPayload;
import org.folio.TestUtil;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.verticle.consumers.errorhandlers.payloadbuilders.MarcAuthorityDiErrorPayloadBuilder;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.HashMap;
import java.util.UUID;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.verticle.consumers.errorhandlers.RawMarcChunksErrorHandler.ERROR_KEY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(VertxUnitRunner.class)
public class MarcAuthorityPayloadBuilderTest {

  private static final String TENANT_ID = "diku";
  private static final String TOKEN = "token";
  private static final String JOB_EXECUTION_ID = UUID.randomUUID().toString();
  private static final String PARSED_RECORD_PATH =
    "src/test/resources/org/folio/services/afterprocessing/parsedRecord.json";
  private static final String LARGE_PAYLOAD_ERROR_MESSAGE = "Record size is greater that MAX_REQUEST_SIZE";

  private MarcAuthorityDiErrorPayloadBuilder payloadBuilder = new MarcAuthorityDiErrorPayloadBuilder();

  @Test
  public void checkEligible() {
    boolean eligible = payloadBuilder.isEligible(Record.RecordType.MARC_AUTHORITY);
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

    Future<DataImportEventPayload> payloadFuture =
      payloadBuilder.buildEventPayload(new RecordTooLargeException(LARGE_PAYLOAD_ERROR_MESSAGE),
        getOkapiParams(), JOB_EXECUTION_ID, record);

    payloadFuture.onComplete(ar -> {
      DataImportEventPayload result = ar.result();
      assertEquals(DI_ERROR.value(), result.getEventType());
      assertTrue(result.getContext().containsKey(ERROR_KEY));

      Record resRecordWithTitle = getRecordFromContext(result);
      assertNull(resRecordWithTitle.getParsedRecord());
      async.complete();
    });
  }

  @Test
  public void shouldBuildPayloadWhenTitleNotExistsInParsedRecord(TestContext context) throws IOException {
    Async async = context.async();
    Record record = new Record().withRecordType(Record.RecordType.MARC_AUTHORITY).withParsedRecord(
      new ParsedRecord().withId(UUID.randomUUID().toString())
        .withContent("{\"leader\":\"01240cas a2200397   4500\",\"fields\":[]}"));

    Future<DataImportEventPayload> payloadFuture =
      payloadBuilder.buildEventPayload(new RecordTooLargeException(LARGE_PAYLOAD_ERROR_MESSAGE),
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

  private ConnectionParams getOkapiParams() {
    HashMap<String, String> headers = new HashMap<>();
    headers.put(XOkapiHeaders.URL, "http://localhost");
    headers.put(XOkapiHeaders.TENANT, TENANT_ID);
    headers.put(XOkapiHeaders.TOKEN, TOKEN);
    return new ConnectionParams(headers);
  }

  private Record getRecordFromFile() throws IOException {
    String parsedRecordContent = TestUtil.readFileFromPath(PARSED_RECORD_PATH);
    return new Record()
      .withRecordType(Record.RecordType.MARC_AUTHORITY)
      .withParsedRecord(new ParsedRecord().withContent(parsedRecordContent));
  }

  private Record getRecordFromContext(DataImportEventPayload eventPayload) {
    String recordStr = eventPayload.getContext().get(EntityType.MARC_AUTHORITY.value());
    return Json.decodeValue(recordStr, Record.class);
  }
}
