package org.folio.verticle.consumers;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.restassured.RestAssured;
import io.vertx.core.json.Json;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ObserveKeyValues;
import net.mguenther.kafka.junit.SendKeyValues;
import org.apache.http.HttpStatus;
import org.folio.DataImportEventPayload;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfile;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_PARSED_RECORDS_CHUNK_SAVED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(VertxUnitRunner.class)
public class StoredRecordChunkConsumersVerticleTest extends AbstractRestTest {
  private static final String JOB_EXECUTION_ID_HEADER = "jobExecutionId";
  private static final String ERROR_MSG_KEY = "ERROR";
  private static final String JOB_PROFILE_ID = UUID.randomUUID().toString();
  private static final String JOB_PROFILE_PATH = "/jobProfile";
  private static final String JOB_PROFILE_SNAPSHOT_ID = "JOB_PROFILE_SNAPSHOT_ID";
  private JobExecution jobExec;

  @Before
  public void setUp() {
    WireMock.stubFor(WireMock.get("/data-import-profiles/jobProfiles/" + JOB_PROFILE_ID + "?withRelations=false&")
      .willReturn(WireMock.ok().withBody(Json.encode(new JobProfile().withId(JOB_PROFILE_ID).withName("Create instance")))));

    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    jobExec = createdJobExecutions.get(0);

    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(JOB_PROFILE_ID)
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExec.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);
  }

  @Test
  public void shouldPublishDiErrorWhenLeaderRecordTypeValueIsInvalid() throws InterruptedException, IOException {
    // given
    // parsed content with invalid value '7' as record type identifier at the LEADER/06
    String parsedContentWithInvalidRecordTypeValue = "{\"leader\": \"13112c7m a2200553Ii 4500\"}";

    RecordsBatchResponse recordsBatch = new RecordsBatchResponse()
      .withTotalRecords(1)
      .withRecords(List.of(new Record()
        .withRecordType(MARC_BIB)
        .withId(UUID.randomUUID().toString())
        .withSnapshotId(jobExec.getId())
        .withParsedRecord(new ParsedRecord().withContent(parsedContentWithInvalidRecordTypeValue))));

    Event event = new Event().withId(UUID.randomUUID().toString()).withEventPayload(Json.encode(recordsBatch));
    KeyValue<String, String> kafkaRecord = new KeyValue<>("42", Json.encode(event));
    kafkaRecord.addHeader(OKAPI_TENANT_HEADER, TENANT_ID, UTF_8);
    kafkaRecord.addHeader(OKAPI_TOKEN_HEADER, TOKEN, UTF_8);
    kafkaRecord.addHeader(JOB_EXECUTION_ID_HEADER, jobExec.getId(), UTF_8);

    String topic = formatToKafkaTopicName(DI_PARSED_RECORDS_CHUNK_SAVED.value());
    SendKeyValues<String, String> request = SendKeyValues.to(topic, Collections.singletonList(kafkaRecord))
      .useDefaults();

    // when
    kafkaCluster.send(request);

    // then
    String topicToObserve = formatToKafkaTopicName(DI_ERROR.value());
    List<String> observedValues = kafkaCluster.observeValues(ObserveKeyValues.on(topicToObserve, 1)
      .observeFor(30, TimeUnit.SECONDS)
      .build());

    Event obtainedEvent = Json.decodeValue(observedValues.get(0), Event.class);
    DataImportEventPayload eventPayload = Json.decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);
    assertEquals(DI_ERROR.value(), eventPayload.getEventType());
    assertEquals(jobExec.getId(), eventPayload.getJobExecutionId());
    assertEquals(TENANT_ID, eventPayload.getTenant());
    assertNotNull(eventPayload.getContext().get(EntityType.MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(eventPayload.getContext().get(ERROR_MSG_KEY));
  }

  @Test
  public void shouldSendEventsWithRecords() throws InterruptedException {
    // given
    String parsedContent = "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"003\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";

    RecordsBatchResponse recordsBatch = new RecordsBatchResponse()
      .withTotalRecords(1)
      .withRecords(List.of(new Record()
        .withRecordType(MARC_BIB)
        .withId(UUID.randomUUID().toString())
        .withSnapshotId(jobExec.getId())
        .withParsedRecord(new ParsedRecord().withContent(parsedContent))));

    Event event = new Event().withId(UUID.randomUUID().toString()).withEventPayload(Json.encode(recordsBatch));
    KeyValue<String, String> kafkaRecord = new KeyValue<>("42", Json.encode(event));
    kafkaRecord.addHeader(OKAPI_TENANT_HEADER, TENANT_ID, UTF_8);
    kafkaRecord.addHeader(OKAPI_TOKEN_HEADER, TOKEN, UTF_8);
    kafkaRecord.addHeader(JOB_EXECUTION_ID_HEADER, jobExec.getId(), UTF_8);

    String topic = formatToKafkaTopicName(DI_PARSED_RECORDS_CHUNK_SAVED.value());
    SendKeyValues<String, String> request = SendKeyValues.to(topic, Collections.singletonList(kafkaRecord))
      .useDefaults();

    // when
    kafkaCluster.send(request);

    // then
    String topicToObserve = formatToKafkaTopicName(DI_SRS_MARC_BIB_RECORD_CREATED.value());
    List<String> observedValues = kafkaCluster.observeValues(ObserveKeyValues.on(topicToObserve, 1)
      .observeFor(30, TimeUnit.SECONDS)
      .build());

    Event obtainedEvent = Json.decodeValue(observedValues.get(0), Event.class);
    DataImportEventPayload eventPayload = Json.decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);
    assertEquals(DI_SRS_MARC_BIB_RECORD_CREATED.value(), eventPayload.getEventType());
    assertEquals(jobExec.getId(), eventPayload.getJobExecutionId());
    assertEquals(TENANT_ID, eventPayload.getTenant());
    assertNotNull(eventPayload.getContext().get(EntityType.MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(eventPayload.getContext().get(JOB_PROFILE_SNAPSHOT_ID));
  }
}
