package org.folio.verticle.consumers;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.restassured.RestAssured;
import io.vertx.core.json.Json;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.folio.DataImportEventPayload;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.folio.KafkaUtil.checkKafkaEventSent;
import static org.folio.KafkaUtil.getValues;
import static org.folio.KafkaUtil.sendEvent;
import static org.folio.dataimport.util.RestUtil.OKAPI_TOKEN_HEADER;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_PARSED_RECORDS_CHUNK_SAVED;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
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
  private final String DI_PARSED_RECORDS_CHUNK_SAVED_TOPIC = formatToKafkaTopicName(DI_PARSED_RECORDS_CHUNK_SAVED.value());

  @Before
  public void setUp() {
    WireMock.stubFor(WireMock.get("/data-import-profiles/jobProfiles/" + JOB_PROFILE_ID + "?withRelations=false&")
      .willReturn(WireMock.ok().withBody(Json.encode(new JobProfile().withId(JOB_PROFILE_ID).withName("Create instance")))));

    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    jobExec = createdJobExecutions.getFirst();
  }

  @Test
  public void shouldPublishDiErrorWhenLeaderRecordTypeValueIsInvalid() throws InterruptedException, ExecutionException {
    linkJobProfileToJobExecution();
    // given
    String parsedContentWithInvalidRecordTypeValue = "{\"leader\": \"13112c7m a2200553Ii 3900\"}";
    RecordsBatchResponse recordsBatch = getRecordsBatchResponse(parsedContentWithInvalidRecordTypeValue, 1);

    ProducerRecord<String, String> producerRecord = getRequest(jobExec.getId(), recordsBatch);

    // when
    sendEvent(producerRecord);

    // then
    List<String> obtainedValues = observeValuesAndFilterByLeader("13112c7m a2200553Ii 3900", DI_ERROR, 1);
    Event obtainedEvent = Json.decodeValue(obtainedValues.getFirst(), Event.class);
    DataImportEventPayload eventPayload = Json.decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);
    assertEquals(DI_ERROR.value(), eventPayload.getEventType());
    assertEquals(TENANT_ID, eventPayload.getTenant());
    assertNotNull(eventPayload.getContext().get(EntityType.MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(eventPayload.getContext().get(ERROR_MSG_KEY));
  }

  @Test
  public void shouldPublishCoupleDiErrorsWhenWrongPayload() throws InterruptedException, ExecutionException {
    linkJobProfileToJobExecution();
    String wrongPayload = "{\"leader\": \"13112c7m a2200553Ii 4300\"}";
    RecordsBatchResponse recordsBatch = getRecordsBatchResponse(wrongPayload, 7);

    ProducerRecord<String, String> producerRecord = getRequest(jobExec.getId(), recordsBatch);

    // when
    sendEvent(producerRecord);

    // then
    List<String> diErrorValues = observeValuesAndFilterByLeader("13112c7m a2200553Ii 4300", DI_ERROR, 7);
    assertEquals(7, diErrorValues.size());
  }

  @Test
  public void shouldPublishCoupleOfSuccessEventsAndCoupleOfDiErrorEvents() throws InterruptedException, ExecutionException {
    linkJobProfileToJobExecution();
    String correctContent = "{\"leader\":\"00116nam  22000731a 4700\",\"fields\":[{\"003\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    String wrongContent = "{\"leader\": \"13113c7m a2200553Ii 4800\"}";

    RecordsBatchResponse correctRecords = getRecordsBatchResponse(correctContent, 3);
    RecordsBatchResponse wrongRecords = getRecordsBatchResponse(wrongContent, 7);

    RecordsBatchResponse allRecords = new RecordsBatchResponse().withTotalRecords(10)
      .withRecords(Stream.concat(correctRecords.getRecords().stream(), wrongRecords.getRecords().stream()).toList());

    ProducerRecord<String, String> producerRecord = getRequest(jobExec.getId(), allRecords);

    // when
    sendEvent(producerRecord);

    // then
    List<String> successValues = observeValuesAndFilterByLeader("00116nam  22000731a 4700", DI_INCOMING_MARC_BIB_RECORD_PARSED, 3);
    assertEquals(3, successValues.size());

    List<String> diErrorValues = observeValuesAndFilterByLeader("13113c7m a2200553Ii 4800", DI_ERROR, 7);
    assertEquals(7, diErrorValues.size());
  }

  @Test
  public void shouldSendEventsWithRecords() throws InterruptedException, ExecutionException {
    linkJobProfileToJobExecution();
    // given
    String parsedContent = "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"003\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    RecordsBatchResponse recordsBatch = getRecordsBatchResponse(parsedContent, 1);

    ProducerRecord<String, String> producerRecord = getRequest(jobExec.getId(), recordsBatch);

    // when
    sendEvent(producerRecord);

    // then
    List<String> observedValues = observeValuesAndFilterByLeader("00115nam  22000731a 4500", DI_INCOMING_MARC_BIB_RECORD_PARSED, 1);
    Event obtainedEvent = Json.decodeValue(observedValues.getFirst(), Event.class);
    DataImportEventPayload eventPayload = Json.decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);
    assertEquals(DI_INCOMING_MARC_BIB_RECORD_PARSED.value(), eventPayload.getEventType());
    assertEquals(TENANT_ID, eventPayload.getTenant());
    assertNotNull(eventPayload.getContext().get(EntityType.MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(eventPayload.getContext().get(JOB_PROFILE_SNAPSHOT_ID));
  }

  @Test
  public void shouldObserveOnlySingleEventInCaseOfDuplicates() throws InterruptedException, ExecutionException {
    linkJobProfileToJobExecution();
    // given
    String parsedContent = "{\"leader\":\"00115nam  22000731a 4500\",\"fields\":[{\"003\":\"in001\"},{\"507\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"data\"}],\"ind1\":\" \",\"ind2\":\" \"}}]}";
    RecordsBatchResponse recordsBatch = getRecordsBatchResponse(parsedContent, 1);

    ProducerRecord<String, String> producerRecord = getRequest(jobExec.getId(), recordsBatch);

    // when
    sendEvent(producerRecord);
    sendEvent(producerRecord);

    // then
    List<String> observedValues = observeValuesAndFilterByLeader("00115nam  22000731a 4500", DI_INCOMING_MARC_BIB_RECORD_PARSED, 1);
    Event obtainedEvent = Json.decodeValue(observedValues.getFirst(), Event.class);
    DataImportEventPayload eventPayload = Json.decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);
    assertEquals(DI_INCOMING_MARC_BIB_RECORD_PARSED.value(), eventPayload.getEventType());
  }

  private RecordsBatchResponse getRecordsBatchResponse(String parsedContent, Integer totalRecords) {
    List<Record> records = new ArrayList<>();
    for (int i = 0; i < totalRecords; i++) {
      records.add(new Record()
        .withRecordType(MARC_BIB)
        .withId(UUID.randomUUID().toString())
        .withSnapshotId(UUID.randomUUID().toString())
        .withParsedRecord(new ParsedRecord().withContent(parsedContent)));
    }
    return new RecordsBatchResponse()
      .withTotalRecords(totalRecords)
      .withRecords(records);
  }

  private ProducerRecord<String, String> getRequest(String jobExecutionId, RecordsBatchResponse recordsBatch) {
    Event event = new Event().withId(UUID.randomUUID().toString()).withEventPayload(Json.encode(recordsBatch));

    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
      DI_PARSED_RECORDS_CHUNK_SAVED_TOPIC,
      "42",
      Json.encode(event)
    );

    producerRecord.headers().add(OKAPI_TENANT_HEADER, TENANT_ID.getBytes(UTF_8));
    producerRecord.headers().add(OKAPI_TOKEN_HEADER, TOKEN.getBytes(UTF_8));
    producerRecord.headers().add(JOB_EXECUTION_ID_HEADER, jobExecutionId.getBytes(UTF_8));

    return producerRecord;
  }

  private List<String> observeValuesAndFilterByLeader(String leader, DataImportEventTypes eventType, Integer countToObserve) {
    String topicToObserve = formatToKafkaTopicName(eventType.value());
    List<String> result = new ArrayList<>();
    List<String> observedValues = getValues(checkKafkaEventSent(topicToObserve, countToObserve, 30, TimeUnit.SECONDS));
    for (String observedValue : observedValues) {
      if (observedValue.contains(leader)) {
        result.add(observedValue);
      }
    }
    return result;
  }

  private void linkJobProfileToJobExecution() {
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
}
