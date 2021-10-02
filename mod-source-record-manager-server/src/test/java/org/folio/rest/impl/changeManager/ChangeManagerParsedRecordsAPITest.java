package org.folio.rest.impl.changeManager;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.notFound;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.serverError;
import static org.hamcrest.Matchers.is;

import static org.folio.kafka.KafkaTopicNameHelper.formatTopicName;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import net.mguenther.kafka.junit.ObserveKeyValues;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.folio.kafka.KafkaConfig;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.verticle.consumers.util.QMEventTypes;

@RunWith(VertxUnitRunner.class)
public class ChangeManagerParsedRecordsAPITest extends AbstractRestTest {

  private final String SOURCE_RECORDS_URL = "/source-storage/source-records/";
  private final String PARSED_RECORDS_URL = "/change-manager/parsedRecords";
  private final String EXTERNAL_ID_QUERY_PARAM = "externalId";

  private static final String KAFKA_HOST = "KAFKA_HOST";
  private static final String KAFKA_PORT = "KAFKA_PORT";
  private static final String KAFKA_ENV = "ENV";
  private static final String KAFKA_ENV_ID = "test-env";
  private static final String KAFKA_MAX_REQUEST_SIZE = "MAX_REQUEST_SIZE";

  KafkaConfig kafkaConfig;

  @Before
  public void setUp() throws Exception {
    String[] hostAndPort = kafkaCluster.getBrokerList().split(":");
    System.setProperty(KAFKA_HOST, hostAndPort[0]);
    System.setProperty(KAFKA_PORT, hostAndPort[1]);
    System.setProperty(KAFKA_ENV, KAFKA_ENV_ID);
    System.setProperty(OKAPI_URL_ENV, OKAPI_URL);
    System.setProperty(KAFKA_MAX_REQUEST_SIZE, "1048576");

    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(hostAndPort[0])
      .kafkaPort(hostAndPort[1])
      .envId(KAFKA_ENV_ID)
      .build();
  }

  @Test
  public void shouldReturnParsedRecordDtoIfSourceRecordExists(TestContext testContext) {
    Async async = testContext.async();

    String externalId = UUID.randomUUID().toString();
    SourceRecord sourceRecord = new SourceRecord()
      .withRecordId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withId(UUID.randomUUID().toString())
        .withContent("{\"leader\":\"01240cas a2200397   4500\",\"fields\":[]}"))
      .withRecordType(SourceRecord.RecordType.MARC_BIB)
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(externalId));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_RECORDS_URL + ".*"), true))
      .willReturn(ok().withBody(JsonObject.mapFrom(sourceRecord).encode())));

    RestAssured.given()
      .spec(spec)
      .queryParam(EXTERNAL_ID_QUERY_PARAM, externalId)
      .when()
      .get(PARSED_RECORDS_URL)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("id", is(sourceRecord.getRecordId()))
      .body("parsedRecord.id", is(sourceRecord.getParsedRecord().getId()))
      .body("recordType", is(sourceRecord.getRecordType().value()))
      .body("externalIdsHolder.instanceId", is(externalId));
    async.complete();
  }

  @Test
  public void shouldReturnNotFoundIfThereIsNoSourceRecord(TestContext testContext) {
    Async async = testContext.async();

    String externalId = UUID.randomUUID().toString();

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_RECORDS_URL + ".*"), true))
      .willReturn(notFound()));

    RestAssured.given()
      .spec(spec)
      .queryParam(EXTERNAL_ID_QUERY_PARAM, externalId)
      .when()
      .get(PARSED_RECORDS_URL)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
    async.complete();
  }

  @Test
  public void shouldReturnErrorIfExceptionWasThrown(TestContext testContext) {
    Async async = testContext.async();

    String externalId = UUID.randomUUID().toString();

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_RECORDS_URL + ".*"), true))
      .willReturn(ok().withBody("{\"leader\":\"01240cas a2200397   4500\",\"fields\":[]}")));

    RestAssured.given()
      .spec(spec)
      .queryParam(EXTERNAL_ID_QUERY_PARAM, externalId)
      .when()
      .get(PARSED_RECORDS_URL)
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    async.complete();
  }

  @Test
  public void shouldReturnErrorIfErrorResponse(TestContext testContext) {
    Async async = testContext.async();

    String externalId = UUID.randomUUID().toString();

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_RECORDS_URL + ".*"), true))
      .willReturn(serverError()));

    RestAssured.given()
      .spec(spec)
      .queryParam(EXTERNAL_ID_QUERY_PARAM, externalId)
      .when()
      .get(PARSED_RECORDS_URL)
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    async.complete();
  }

  @Test
  public void shouldUpdateParsedRecordOnPut(TestContext testContext) throws InterruptedException {
    Async async = testContext.async();

    ParsedRecordDto parsedRecordDto = new ParsedRecordDto()
      .withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withId(UUID.randomUUID().toString())
        .withContent("{\"leader\":\"01240cas a2200397   4500\",\"fields\":[]}"))
      .withRecordType(ParsedRecordDto.RecordType.MARC_HOLDING)
      .withQmRecordVersion("1")
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(UUID.randomUUID().toString()));

    RestAssured.given()
      .spec(spec)
      .body(parsedRecordDto)
      .when()
      .put(PARSED_RECORDS_URL + "/" + parsedRecordDto.getId())
      .then()
      .statusCode(HttpStatus.SC_ACCEPTED);

    String observeTopic =
      formatTopicName(kafkaConfig.getEnvId(), getDefaultNameSpace(), TENANT_ID, QMEventTypes.QM_RECORD_UPDATED.name());
    kafkaCluster.observeValues(ObserveKeyValues.on(observeTopic, 1)
      .observeFor(30, TimeUnit.SECONDS)
      .build());

    async.complete();
  }
}
