package org.folio.rest.impl.changeManager;

import static java.util.Collections.emptyList;
import static org.folio.KafkaUtil.checkKafkaEventSent;
import static org.folio.KafkaUtil.getKafkaHostAndPort;
import static org.folio.kafka.KafkaTopicNameHelper.formatTopicName;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.restassured.RestAssured;
import io.vertx.core.json.Json;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.UUID;
import org.apache.http.HttpStatus;
import org.folio.kafka.KafkaConfig;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.verticle.consumers.util.QMEventTypes;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class ChangeManagerParsedRecordsAPITest extends AbstractRestTest {

  private static final String KAFKA_HOST = "KAFKA_HOST";
  private static final String KAFKA_PORT = "KAFKA_PORT";
  private static final String KAFKA_ENV = "ENV";
  private static final String KAFKA_ENV_ID = "test-env";
  private static final String KAFKA_MAX_REQUEST_SIZE = "MAX_REQUEST_SIZE";
  private static final String PARSED_RECORDS_URL = "/change-manager/parsedRecords";

  private KafkaConfig kafkaConfig;

  @Before
  public void setUp() {
    String[] hostAndPort = getKafkaHostAndPort();
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
  public void shouldUpdateParsedRecordOnPut(TestContext testContext) {
    Async async = testContext.async();

    WireMock.stubFor(WireMock.get("/linking-rules/instance-authority")
      .willReturn(WireMock.ok().withBody(Json.encode(emptyList()))));

    ParsedRecordDto parsedRecordDto = new ParsedRecordDto()
      .withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withId(UUID.randomUUID().toString())
        .withContent("{\"leader\":\"01240cas a2200397   4500\",\"fields\":[]}"))
      .withRecordType(ParsedRecordDto.RecordType.MARC_HOLDING)
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
    checkKafkaEventSent(observeTopic, 1);

    async.complete();
  }
}
