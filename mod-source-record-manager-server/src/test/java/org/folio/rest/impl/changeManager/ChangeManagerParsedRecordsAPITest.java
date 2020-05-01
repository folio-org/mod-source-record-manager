package org.folio.rest.impl.changeManager;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class ChangeManagerParsedRecordsAPITest extends AbstractRestTest {

  private final String SOURCE_RECORDS_URL = "/source-storage/sourceRecords/";
  private final String PARSED_RECORDS_URL = "/change-manager/parsedRecords";
  private final String INSTANCE_ID_QUERY_PARAM = "instanceId";

  @Test
  public void shouldReturnParsedRecordDtoIfSourceRecordExists(TestContext testContext) {
    Async async = testContext.async();

    String instanceId = UUID.randomUUID().toString();
    SourceRecord sourceRecord = new SourceRecord()
      .withRecordId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withId(UUID.randomUUID().toString())
        .withContent("{\"leader\":\"01240cas a2200397   4500\",\"fields\":[]}"))
      .withRecordType(SourceRecord.RecordType.MARC)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_RECORDS_URL + ".*"), true))
      .willReturn(ok().withBody(JsonObject.mapFrom(sourceRecord).encode())));

    RestAssured.given()
      .spec(spec)
      .queryParam(INSTANCE_ID_QUERY_PARAM, instanceId)
      .when()
      .get(PARSED_RECORDS_URL)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(sourceRecord.getRecordId()))
      .body("parsedRecord.id", is(sourceRecord.getParsedRecord().getId()))
      .body("recordType", is(sourceRecord.getRecordType().value()))
      .body("externalIdsHolder.instanceId", is(instanceId));
    async.complete();
  }

  @Test
  public void shouldReturnNotFoundIfThereIsNoSourceRecord(TestContext testContext) {
    Async async = testContext.async();

    String instanceId = UUID.randomUUID().toString();

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_RECORDS_URL + ".*"), true))
      .willReturn(notFound()));

    RestAssured.given()
      .spec(spec)
      .queryParam(INSTANCE_ID_QUERY_PARAM, instanceId)
      .when()
      .get(PARSED_RECORDS_URL)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
    async.complete();
  }

  @Test
  public void shouldReturnErrorIfExceptionWasThrown(TestContext testContext) {
    Async async = testContext.async();

    String instanceId = UUID.randomUUID().toString();

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_RECORDS_URL + ".*"), true))
      .willReturn(ok().withBody("{\"leader\":\"01240cas a2200397   4500\",\"fields\":[]}")));

    RestAssured.given()
      .spec(spec)
      .queryParam(INSTANCE_ID_QUERY_PARAM, instanceId)
      .when()
      .get(PARSED_RECORDS_URL)
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    async.complete();
  }

}
