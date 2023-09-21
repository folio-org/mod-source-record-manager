package org.folio.verticle.consumers;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.restassured.RestAssured;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ObserveKeyValues;
import net.mguenther.kafka.junit.ReadKeyValues;
import net.mguenther.kafka.junit.SendKeyValues;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.http.HttpStatus;
import org.folio.MatchDetail;
import org.folio.MatchProfile;
import org.folio.TestUtil;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.ActionProfile;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.JobProfile;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.MappingProfile;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsMetadata;
import org.folio.verticle.consumers.errorhandlers.RawMarcChunksErrorHandler;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.*;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.*;
import static org.folio.rest.jaxrs.model.Record.RecordType.EDIFACT;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

@RunWith(VertxUnitRunner.class)
public class RawMarcChunkConsumersVerticleTest extends AbstractRestTest {

  private static final String RAW_RECORD_WITH_999_ff_field = "00948nam a2200241 a 4500001000800000003000400008005001700012008004100029035002100070035002000091040002300111041001300134100002300147245007900170260005800249300002400307440007100331650003600402650005500438650006900493655006500562999007900627\u001E1007048\u001EICU\u001E19950912000000.0\u001E891218s1983    wyu      d    00010 eng d\u001E  \u001Fa(ICU)BID12424550\u001E  \u001Fa(OCoLC)16105467\u001E  \u001FaPAU\u001FcPAU\u001Fdm/c\u001FdICU\u001E0 \u001Faeng\u001Faarp\u001E1 \u001FaSalzmann, Zdeněk\u001E10\u001FaDictionary of contemporary Arapaho usage /\u001Fccompiled by Zdeněk Salzmann.\u001E0 \u001FaWind River, Wyoming :\u001FbWind River Reservation,\u001Fc1983.\u001E  \u001Fav, 231 p. ;\u001Fc28 cm.\u001E 0\u001FaArapaho language and culture instructional materials series\u001Fvno. 4\u001E 0\u001FaArapaho language\u001FxDictionaries.\u001E 0\u001FaIndians of North America\u001FxLanguages\u001FxDictionaries.\u001E 7\u001FaArapaho language.\u001F2fast\u001F0http://id.worldcat.org/fast/fst00812722\u001E 7\u001FaDictionaries.\u001F2fast\u001F0http://id.worldcat.org/fast/fst01423826\u001Eff\u001Fie27a5374-0857-462e-ac84-fb4795229c7a\u001Fse27a5374-0857-462e-ac84-fb4795229c7a\u001E\u001D";
  private static final String RAW_EDIFACT_RECORD_PATH = "src/test/resources/records/edifact/565751us20210122.edi";
  private static final String JOB_PROFILE_PATH = "/jobProfile";
  private static final String JOB_EXECUTION_ID_HEADER = "jobExecutionId";
  private static final String JOB_PROFILE_ID = UUID.randomUUID().toString();
  private static final String GROUP_ID = "test-consumers";
  private static String rawEdifactContent;

  private ActionProfile updateInstanceActionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Update instance")
    .withAction(ActionProfile.Action.UPDATE)
    .withFolioRecord(ActionProfile.FolioRecord.INSTANCE);

  private ProfileSnapshotWrapper updateInstanceJobProfileSnapshot = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withContentType(JOB_PROFILE)
    .withContent(jobProfile)
    .withChildSnapshotWrappers(List.of(
      new ProfileSnapshotWrapper()
        .withContentType(ACTION_PROFILE)
        .withContent(updateInstanceActionProfile)));

  private MatchProfile matchProfileMarcBibToInstance =
    new MatchProfile().withMatchDetails(List.of(new MatchDetail()
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(EntityType.INSTANCE)));

  private MatchProfile matchProfileMarcBibToMarcBib =
    new MatchProfile().withMatchDetails(List.of(new MatchDetail()
      .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(EntityType.MARC_BIBLIOGRAPHIC)));

  private MappingProfile mappingProfileMarcBibToMarcBib = new MappingProfile()
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(EntityType.MARC_BIBLIOGRAPHIC);

  private ActionProfile createAuthorityActionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create authority")
    .withAction(ActionProfile.Action.CREATE)
    .withFolioRecord(ActionProfile.FolioRecord.AUTHORITY);

  private ProfileSnapshotWrapper marcBibUpdateUnsupportedSimpleJobProfileSnapshot = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withContentType(JOB_PROFILE)
    .withContent(jobProfile)
    .withChildSnapshotWrappers(List.of(
      new ProfileSnapshotWrapper().withContentType(MATCH_PROFILE).withContent(matchProfileMarcBibToInstance)
        .withChildSnapshotWrappers(List.of(
          new ProfileSnapshotWrapper().withContentType(ACTION_PROFILE)
            .withChildSnapshotWrappers(List.of(
              new ProfileSnapshotWrapper().withContentType(MAPPING_PROFILE).withContent(mappingProfileMarcBibToMarcBib))
            )))));

  private ProfileSnapshotWrapper marcBibUpdateUnsupportedJobProfileSnapshot = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withContentType(JOB_PROFILE)
    .withContent(jobProfile)
    .withChildSnapshotWrappers(List.of(
      new ProfileSnapshotWrapper().withContentType(MATCH_PROFILE).withContent(matchProfileMarcBibToMarcBib)
        .withChildSnapshotWrappers(List.of(
          new ProfileSnapshotWrapper().withContentType(MATCH_PROFILE).withContent(matchProfileMarcBibToInstance)
            .withChildSnapshotWrappers(List.of(
              new ProfileSnapshotWrapper().withContentType(ACTION_PROFILE)
                .withChildSnapshotWrappers(List.of(
                  new ProfileSnapshotWrapper().withContentType(MAPPING_PROFILE).withContent(mappingProfileMarcBibToMarcBib))
                )))))
    ));

  @BeforeClass
  public static void setUpClass() throws IOException {
    rawEdifactContent = TestUtil.readFileFromPath(RAW_EDIFACT_RECORD_PATH);
  }

  @Before
  public void setUp() {
    WireMock.stubFor(WireMock.get("/data-import-profiles/jobProfiles/" + JOB_PROFILE_ID + "?withRelations=false&")
      .willReturn(WireMock.ok().withBody(Json.encode(new JobProfile().withId(JOB_PROFILE_ID).withName("Create instance")))));
    WireMock.stubFor(WireMock.post("/source-storage/batch/verified-records")
      .willReturn(WireMock.ok().withBody(Json.encode(new JsonObject("{\"invalidMarcBibIds\" : [ \"111111\", \"222222\" ]}")))));
    WireMock.stubFor(WireMock.get("/linking-rules/instance-authority")
      .willReturn(WireMock.ok().withBody(Json.encode(emptyList()))));
  }

  @Test
  public void shouldFillInInstanceIdAndInstanceHridWhenRecordContains999FieldWithInstanceId() throws InterruptedException {
    // given
    SendKeyValues<String, String> request = prepareWithSpecifiedRecord(JobProfileInfo.DataType.MARC, RecordsMetadata.ContentType.MARC_RAW, RAW_RECORD_WITH_999_ff_field);

    // when
    kafkaCluster.send(request);

    // then
    Event obtainedEvent = checkEventWithTypeSent(DI_RAW_RECORDS_CHUNK_PARSED);
    RecordCollection recordCollection = Json.decodeValue(obtainedEvent.getEventPayload(), RecordCollection.class);
    assertEquals(1, recordCollection.getRecords().size());
    Record record = recordCollection.getRecords().get(0);
    assertNotNull(record.getExternalIdsHolder());
    assertEquals("e27a5374-0857-462e-ac84-fb4795229c7a", record.getExternalIdsHolder().getInstanceId());
    assertEquals("1007048", record.getExternalIdsHolder().getInstanceHrid());
  }

  @Test
  public void shouldParseAndPublishChunkWithEdifactRecord() throws InterruptedException {
    // given
    WireMock.stubFor(post(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.created().withBody(Json.encode(createInvoiceProfileSnapshotWrapperResponse))));
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(createInvoiceProfileSnapshotWrapperResponse))));
    SendKeyValues<String, String> request = prepareWithSpecifiedRecord(JobProfileInfo.DataType.EDIFACT, RecordsMetadata.ContentType.EDIFACT_RAW, rawEdifactContent);

    // when
    kafkaCluster.send(request);

    // then
    Event obtainedEvent = checkEventWithTypeSent(DI_RAW_RECORDS_CHUNK_PARSED);
    RecordCollection recordCollection = Json.decodeValue(obtainedEvent.getEventPayload(), RecordCollection.class);
    assertEquals(1, recordCollection.getRecords().size());
    Record record = recordCollection.getRecords().get(0);
    assertEquals(EDIFACT, record.getRecordType());
  }

  @Test
  public void shouldNotObserveValuesWhenEventPayloadNotParsed() throws InterruptedException {
    // given
    SendKeyValues<String, String> request = prepareWithSpecifiedEventPayload(JobProfileInfo.DataType.MARC, "errorPayload");
    String jobExecutionId = getJobExecutionId(request);

    // when
    kafkaCluster.send(request);

    // then
    checkEventWithTypeWasNotSend(jobExecutionId, DI_RAW_RECORDS_CHUNK_PARSED);
    checkDiErrorEventsSent(jobExecutionId,"Failed to decode:Unrecognized token 'errorPayload': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false')");
  }

  @Test
  public void shouldCreateErrorRecordsWhenRecordNotParsed() throws InterruptedException {
    // given
    SendKeyValues<String, String> request = prepareWithSpecifiedRecord(JobProfileInfo.DataType.MARC, RecordsMetadata.ContentType.MARC_RAW, "errorPayload");

    // when
    kafkaCluster.send(request);

    // then
    Event obtainedEvent = checkEventWithTypeSent(DI_RAW_RECORDS_CHUNK_PARSED);
    RecordCollection recordCollection = Json.decodeValue(obtainedEvent.getEventPayload(), RecordCollection.class);
    assertEquals(1, recordCollection.getRecords().size());
    ErrorRecord errorRecord = recordCollection.getRecords().get(0).getErrorRecord();
    assertTrue(errorRecord.getDescription().contains("org.marc4j.MarcException"));
  }

  @Test
  public void shouldGetUnsupportedProfileExceptionSimpleProfile() throws InterruptedException {
    // given
    WireMock.stubFor(post(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.created().withBody(Json.encode(marcBibUpdateUnsupportedSimpleJobProfileSnapshot))));

    SendKeyValues<String, String> request = prepareWithSpecifiedRecord(JobProfileInfo.DataType.MARC,
      RecordsMetadata.ContentType.MARC_RAW, RAW_RECORD_WITH_999_ff_field);

    // when
    kafkaCluster.send(request);

    // then
    Event obtainedEvent = checkEventWithTypeSent(DI_ERROR);
    DataImportEventPayload eventPayload = Json.decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);
    assertTrue(eventPayload.getContext().get(RawMarcChunksErrorHandler.ERROR_KEY).contains("Unsupported"));
  }

  @Test
  public void shouldGetUnsupportedProfileException() throws InterruptedException {
    // given
    WireMock.stubFor(post(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.created().withBody(Json.encode(marcBibUpdateUnsupportedJobProfileSnapshot))));

    SendKeyValues<String, String> request = prepareWithSpecifiedRecord(JobProfileInfo.DataType.MARC,
      RecordsMetadata.ContentType.MARC_RAW, RAW_RECORD_WITH_999_ff_field);

    // when
    kafkaCluster.send(request);

    // then
    Event obtainedEvent = checkEventWithTypeSent(DI_ERROR);
    DataImportEventPayload eventPayload = Json.decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);
    assertTrue(eventPayload.getContext().get(RawMarcChunksErrorHandler.ERROR_KEY).contains("Unsupported"));
  }

  @Test
  public void shouldNotObserveValuesWhenJobExecutionIdNotCreated() throws InterruptedException {
    RawRecordsDto chunk = getChunk(RecordsMetadata.ContentType.MARC_RAW, RAW_RECORD_WITH_999_ff_field);
    String jobExecutionId = UUID.randomUUID().toString();

    Event event = new Event().withId(UUID.randomUUID().toString()).withEventPayload(Json.encode(chunk));
    KeyValue<String, String> kafkaRecord = new KeyValue<>("1", Json.encode(event));
    kafkaRecord.addHeader(OKAPI_TENANT_HEADER, TENANT_ID, UTF_8);
    kafkaRecord.addHeader(OKAPI_URL_HEADER, snapshotMockServer.baseUrl(), UTF_8);
    kafkaRecord.addHeader(JOB_EXECUTION_ID_HEADER, jobExecutionId, UTF_8);

    String topic = formatToKafkaTopicName(DI_RAW_RECORDS_CHUNK_READ.value());
    SendKeyValues<String, String> request = SendKeyValues.to(topic, singletonList(kafkaRecord))
      .useDefaults();

    // when
    kafkaCluster.send(request);

    // then
    checkEventWithTypeWasNotSend(jobExecutionId, DI_RAW_RECORDS_CHUNK_PARSED);
    checkDiErrorEventsSent(jobExecutionId, "Couldn't find JobExecution with id");
  }

  @Test
  public void shouldNotSendAnyEventsForDuplicates() throws InterruptedException {
    // given
    RawRecordsDto chunk = getChunk(RecordsMetadata.ContentType.MARC_RAW, RAW_RECORD_WITH_999_ff_field);
    JobExecutionSourceChunkDao jobExecutionSourceChunkDao = getBeanFromSpringContext(vertx, org.folio.dao.JobExecutionSourceChunkDao.class);
    jobExecutionSourceChunkDao.save(new JobExecutionSourceChunk()
      .withId(chunk.getId())
      .withState(JobExecutionSourceChunk.State.IN_PROGRESS), TENANT_ID);

    SendKeyValues<String, String> request = prepareWithSpecifiedEventPayload(JobProfileInfo.DataType.MARC, Json.encode(chunk));
    String jobExecutionId = getJobExecutionId(request);

    // when
    kafkaCluster.send(request);

    // then
    checkEventWithTypeWasNotSend(jobExecutionId, DI_RAW_RECORDS_CHUNK_PARSED);
    checkEventWithTypeWasNotSend(jobExecutionId, DI_ERROR);
  }

  @Test
  public void shouldNotSendDIErrorWhenReceiveDuplicateEvent() throws InterruptedException {
    // given
    RawRecordsDto chunk = getChunk(RecordsMetadata.ContentType.MARC_RAW, RAW_RECORD_WITH_999_ff_field);
    SendKeyValues<String, String> request = prepareWithSpecifiedEventPayload(JobProfileInfo.DataType.MARC, Json.encode(chunk));
    String jobExecutionId = getJobExecutionId(request);

    // when
    kafkaCluster.send(request);
    kafkaCluster.send(request);

    // then
    checkEventWithTypeSent(DI_RAW_RECORDS_CHUNK_PARSED);
    checkEventWithTypeWasNotSend(jobExecutionId, DI_ERROR);
  }

  @Test
  public void shouldSendEventDiMarcForUpdateReceivedWhenProfileSnapshotContainsUpdateInstanceActionProfile() throws InterruptedException {
    // given
    WireMock.stubFor(post(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.created().withBody(Json.encode(updateInstanceJobProfileSnapshot))));

    SendKeyValues<String, String> request = prepareWithSpecifiedRecord(JobProfileInfo.DataType.MARC,
      RecordsMetadata.ContentType.MARC_RAW, RAW_RECORD_WITH_999_ff_field);

    // when
    kafkaCluster.send(request);

    // then
    Event obtainedEvent = checkEventWithTypeSent(DI_MARC_FOR_UPDATE_RECEIVED);
    DataImportEventPayload eventPayload = Json.decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);
    assertEquals(DI_MARC_FOR_UPDATE_RECEIVED.value(), eventPayload.getEventType());
    assertNotNull(eventPayload.getContext().get(EntityType.MARC_BIBLIOGRAPHIC.value()));
  }

  @Test
  public void shouldSendDIErrorWhenJobProfileIncompatibleWithMarcRecordSubtype() throws InterruptedException {
    // given
    ProfileSnapshotWrapper createAuthorityJobProfileSnapshot = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withContent(jobProfile)
      .withChildSnapshotWrappers(List.of(
        new ProfileSnapshotWrapper()
          .withContentType(ACTION_PROFILE)
          .withContent(createAuthorityActionProfile)));

    WireMock.stubFor(post(new UrlPathPattern(new RegexPattern(PROFILE_SNAPSHOT_URL + "/.*"), true))
      .willReturn(WireMock.created().withBody(Json.encode(createAuthorityJobProfileSnapshot))));

    SendKeyValues<String, String> request = prepareWithSpecifiedRecord(JobProfileInfo.DataType.MARC,
      RecordsMetadata.ContentType.MARC_RAW, RAW_RECORD_WITH_999_ff_field);

    // when
    kafkaCluster.send(request);

    // then
    Event obtainedEvent = checkEventWithTypeSent(DI_ERROR);
    DataImportEventPayload eventPayload = Json.decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);
    assertEquals(DI_ERROR.value(), eventPayload.getEventType());
  }

  private SendKeyValues<String, String> prepareWithSpecifiedRecord(JobProfileInfo.DataType dataType,
                                                                   RecordsMetadata.ContentType contentType,
                                                                   String record) {
    RawRecordsDto chunk = getChunk(contentType, record);

    return prepareWithSpecifiedEventPayload(dataType, Json.encode(chunk));
  }

  private SendKeyValues<String, String> prepareWithSpecifiedEventPayload(JobProfileInfo.DataType dataType,
                                                                         String eventPayload) {
    String jobExecutionId = emulateJobExecutionIdRequest(dataType);

    Event event = new Event().withId(UUID.randomUUID().toString()).withEventPayload(eventPayload);
    KeyValue<String, String> kafkaRecord = new KeyValue<>("key", Json.encode(event));
    kafkaRecord.addHeader(OKAPI_TENANT_HEADER, TENANT_ID, UTF_8);
    kafkaRecord.addHeader(OKAPI_URL_HEADER, snapshotMockServer.baseUrl(), UTF_8);
    kafkaRecord.addHeader(JOB_EXECUTION_ID_HEADER, jobExecutionId, UTF_8);

    String topic = formatToKafkaTopicName(DI_RAW_RECORDS_CHUNK_READ.value());
    return SendKeyValues.to(topic, singletonList(kafkaRecord))
      .useDefaults();
  }

  private RawRecordsDto getChunk(RecordsMetadata.ContentType contentType, String record) {
    return new RawRecordsDto()
      .withId(UUID.randomUUID().toString())
      .withInitialRecords(List.of(new InitialRecord().withRecord(record).withOrder(0)))
      .withRecordsMetadata(new RecordsMetadata()
        .withContentType(contentType)
        .withCounter(1)
        .withLast(false)
        .withTotal(1));
  }

  private String emulateJobExecutionIdRequest(JobProfileInfo.DataType dataType) {
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExecution = createdJobExecutions.get(0);

    RestAssured.given()
      .spec(spec)
      .body(new JobProfileInfo()
        .withName("Records name")
        .withId(JOB_PROFILE_ID)
        .withDataType(dataType))
      .when()
      .put(JOB_EXECUTION_PATH + jobExecution.getId() + JOB_PROFILE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK);

    return jobExecution.getId();
  }

  private String getJobExecutionId(SendKeyValues<String, String> request) {
    return new String(request.getRecords().stream()
      .findFirst()
      .get()
      .getHeaders()
      .lastHeader(JOB_EXECUTION_ID_HEADER).value());
  }

  private Event checkEventWithTypeSent(DataImportEventTypes eventType) throws InterruptedException {
    String topicToObserve = formatToKafkaTopicName(eventType.value());
    List<String> observedValues = kafkaCluster.observeValues(ObserveKeyValues.on(topicToObserve, 1)
      .with(GROUP_ID_CONFIG, GROUP_ID)
      .observeFor(60, TimeUnit.SECONDS)
      .build());
    return Json.decodeValue(observedValues.get(0), Event.class);
  }

  private void checkEventWithTypeWasNotSend(String jobExecutionId, DataImportEventTypes eventType) throws InterruptedException {
    String topicToObserve = formatToKafkaTopicName(eventType.value());
    List<String> observedValues = kafkaCluster.observeValues(ObserveKeyValues.on(topicToObserve, 0)
      .with(GROUP_ID_CONFIG, GROUP_ID)
      .observeFor(10, TimeUnit.SECONDS)
      .build());

    List<DataImportEventPayload> testedEventsPayLoads = filterObservedValues(jobExecutionId, observedValues);

    assertEquals(0, testedEventsPayLoads.size());
  }

  private void checkDiErrorEventsSent(String jobExecutionId, String errorMessage) throws InterruptedException {
    String observeTopic = formatToKafkaTopicName(DI_ERROR.value());
    List<String> observedValues = kafkaCluster.readValues(ReadKeyValues.from(observeTopic).build());
    if (CollectionUtils.isEmpty(observedValues)) {
      observedValues = kafkaCluster.observeValues(ObserveKeyValues.on(observeTopic, 1)
        .observeFor(60, TimeUnit.SECONDS)
        .build());
    }

    List<DataImportEventPayload> testedEventsPayLoads = filterObservedValues(jobExecutionId, observedValues);

    assertEquals(1, testedEventsPayLoads.size());
    for (DataImportEventPayload payload: testedEventsPayLoads) {
      String actualErrorMessage = payload.getContext().get(RawMarcChunksErrorHandler.ERROR_KEY);
      assertTrue(actualErrorMessage.contains(errorMessage));
    }
  }

  private List<DataImportEventPayload> filterObservedValues(String jobExecutionId, List<String> observedValues) {
    List<DataImportEventPayload> result = new ArrayList<>();
    for (String observedValue : observedValues) {
      Event obtainedEvent = Json.decodeValue(observedValue, Event.class);
      DataImportEventPayload eventPayload = Json.decodeValue(obtainedEvent.getEventPayload(), DataImportEventPayload.class);
      if (jobExecutionId.equals(eventPayload.getJobExecutionId())) {
        result.add(eventPayload);
      }
    }
    return result;
  }
}
