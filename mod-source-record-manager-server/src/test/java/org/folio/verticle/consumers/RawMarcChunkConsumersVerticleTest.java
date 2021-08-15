package org.folio.verticle.consumers;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_PARSED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_READ;
import static org.folio.rest.jaxrs.model.Record.RecordType.EDIFACT;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.restassured.RestAssured;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ObserveKeyValues;
import net.mguenther.kafka.junit.SendKeyValues;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.folio.TestUtil;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfile;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsMetadata;

@RunWith(VertxUnitRunner.class)
public class RawMarcChunkConsumersVerticleTest extends AbstractRestTest {

  private static final String RAW_RECORD_WITH_999_ff_field = "00948nam a2200241 a 4500001000800000003000400008005001700012008004100029035002100070035002000091040002300111041001300134100002300147245007900170260005800249300002400307440007100331650003600402650005500438650006900493655006500562999007900627\u001E1007048\u001EICU\u001E19950912000000.0\u001E891218s1983    wyu      d    00010 eng d\u001E  \u001Fa(ICU)BID12424550\u001E  \u001Fa(OCoLC)16105467\u001E  \u001FaPAU\u001FcPAU\u001Fdm/c\u001FdICU\u001E0 \u001Faeng\u001Faarp\u001E1 \u001FaSalzmann, Zdeněk\u001E10\u001FaDictionary of contemporary Arapaho usage /\u001Fccompiled by Zdeněk Salzmann.\u001E0 \u001FaWind River, Wyoming :\u001FbWind River Reservation,\u001Fc1983.\u001E  \u001Fav, 231 p. ;\u001Fc28 cm.\u001E 0\u001FaArapaho language and culture instructional materials series\u001Fvno. 4\u001E 0\u001FaArapaho language\u001FxDictionaries.\u001E 0\u001FaIndians of North America\u001FxLanguages\u001FxDictionaries.\u001E 7\u001FaArapaho language.\u001F2fast\u001F0http://id.worldcat.org/fast/fst00812722\u001E 7\u001FaDictionaries.\u001F2fast\u001F0http://id.worldcat.org/fast/fst01423826\u001Eff\u001Fie27a5374-0857-462e-ac84-fb4795229c7a\u001Fse27a5374-0857-462e-ac84-fb4795229c7a\u001E\u001D";
  private static final String RAW_EDIFACT_RECORD_PATH = "src/test/resources/records/edifact/565751us20210122.edi";
  private static final String JOB_PROFILE_PATH = "/jobProfile";
  private static final String JOB_EXECUTION_ID_HEADER = "jobExecutionId";
  private static final String JOB_PROFILE_ID = UUID.randomUUID().toString();
  private static final String GROUP_ID = "test-consumers";
  private static String rawEdifactContent;

  @BeforeClass
  public static void setUpClass() throws IOException {
    rawEdifactContent = TestUtil.readFileFromPath(RAW_EDIFACT_RECORD_PATH);
  }

  @Before
  public void setUp() {
    WireMock.stubFor(WireMock.get("/data-import-profiles/jobProfiles/" + JOB_PROFILE_ID + "?withRelations=false&")
      .willReturn(WireMock.ok().withBody(Json.encode(new JobProfile().withId(JOB_PROFILE_ID).withName("Create instance")))));
    WireMock.stubFor(WireMock.post("/source-storage/stream/verify")
      .willReturn(WireMock.ok().withBody(Json.encode(new JsonObject("{\"invalidMarcBibIds\" : [ \"111111\", \"222222\" ]}")))));
  }

  @Test
  public void shouldFillInInstanceIdAndInstanceHridWhenRecordContains999FieldWithInstanceId() throws InterruptedException, IOException {
    // given
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExecution = createdJobExecutions.get(0);

    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("MARC records")
        .withId(JOB_PROFILE_ID)
        .withDataType(JobProfileInfo.DataType.MARC))
      .when()
      .put(JOB_EXECUTION_PATH + jobExecution.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);

    RawRecordsDto chunk = new RawRecordsDto()
      .withId(UUID.randomUUID().toString())
      .withInitialRecords(List.of(new InitialRecord().withRecord(RAW_RECORD_WITH_999_ff_field).withOrder(0)))
      .withRecordsMetadata(new RecordsMetadata()
        .withContentType(RecordsMetadata.ContentType.MARC_RAW)
        .withCounter(1)
        .withLast(false)
        .withTotal(1));

    Event event = new Event().withId(UUID.randomUUID().toString()).withEventPayload(ZIPArchiver.zip(Json.encode(chunk)));
    KeyValue<String, String> kafkaRecord = new KeyValue<>("42", Json.encode(event));
    kafkaRecord.addHeader(OKAPI_TENANT_HEADER, TENANT_ID, UTF_8);
    kafkaRecord.addHeader(OKAPI_URL_HEADER, snapshotMockServer.baseUrl(), UTF_8);
    kafkaRecord.addHeader(JOB_EXECUTION_ID_HEADER, jobExecution.getId(), UTF_8);

    String topic = formatToKafkaTopicName(DI_RAW_RECORDS_CHUNK_READ.value());
    SendKeyValues<String, String> request = SendKeyValues.to(topic, Collections.singletonList(kafkaRecord))
      .useDefaults();

    // when
    kafkaCluster.send(request);

    // then
    String topicToObserve = formatToKafkaTopicName(DI_RAW_RECORDS_CHUNK_PARSED.value());
    List<String> observedValues = kafkaCluster.observeValues(ObserveKeyValues.on(topicToObserve, 1)
      .with(GROUP_ID_CONFIG, GROUP_ID)
      .observeFor(60, TimeUnit.SECONDS)
      .build());

    Event obtainedEvent = Json.decodeValue(observedValues.get(0), Event.class);
    RecordCollection recordCollection = Json.decodeValue(ZIPArchiver.unzip(obtainedEvent.getEventPayload()), RecordCollection.class);
    assertEquals(1, recordCollection.getRecords().size());
    Record record = recordCollection.getRecords().get(0);
    assertNotNull(record.getExternalIdsHolder());

    assertEquals("e27a5374-0857-462e-ac84-fb4795229c7a", record.getExternalIdsHolder().getInstanceId());
    assertEquals("1007048", record.getExternalIdsHolder().getInstanceHrid());
  }

  @Test
  public void shouldParseAndPublishChunkWithEdifactRecord() throws InterruptedException, IOException {
    // given
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExecution = createdJobExecutions.get(0);

    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("Create EDIFACT invoice")
        .withId(JOB_PROFILE_ID)
        .withDataType(JobProfileInfo.DataType.EDIFACT))
      .when()
      .put(JOB_EXECUTION_PATH + jobExecution.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);

    RawRecordsDto chunk = new RawRecordsDto()
      .withId(UUID.randomUUID().toString())
      .withInitialRecords(List.of(new InitialRecord().withRecord(rawEdifactContent).withOrder(0)))
      .withRecordsMetadata(new RecordsMetadata()
        .withContentType(RecordsMetadata.ContentType.EDIFACT_RAW)
        .withCounter(1)
        .withLast(false)
        .withTotal(1));

    Event event = new Event().withId(UUID.randomUUID().toString()).withEventPayload(ZIPArchiver.zip(Json.encode(chunk)));
    KeyValue<String, String> kafkaRecord = new KeyValue<>("1", Json.encode(event));
    kafkaRecord.addHeader(OKAPI_TENANT_HEADER, TENANT_ID, UTF_8);
    kafkaRecord.addHeader(OKAPI_URL_HEADER, snapshotMockServer.baseUrl(), UTF_8);
    kafkaRecord.addHeader(JOB_EXECUTION_ID_HEADER, jobExecution.getId(), UTF_8);

    String topic = formatToKafkaTopicName(DI_RAW_RECORDS_CHUNK_READ.value());
    SendKeyValues<String, String> request = SendKeyValues.to(topic, Collections.singletonList(kafkaRecord))
      .useDefaults();

    // when
    kafkaCluster.send(request);

    // then
    String topicToObserve = formatToKafkaTopicName(DI_RAW_RECORDS_CHUNK_PARSED.value());
    List<String> observedValues = kafkaCluster.observeValues(ObserveKeyValues.on(topicToObserve, 1)
      .with(GROUP_ID_CONFIG, GROUP_ID)
      .observeFor(60, TimeUnit.SECONDS)
      .build());

    Event obtainedEvent = Json.decodeValue(observedValues.get(0), Event.class);
    RecordCollection recordCollection = Json.decodeValue(ZIPArchiver.unzip(obtainedEvent.getEventPayload()), RecordCollection.class);
    assertEquals(1, recordCollection.getRecords().size());
    Record record = recordCollection.getRecords().get(0);
    assertEquals(EDIFACT, record.getRecordType());
  }

}
