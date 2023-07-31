package org.folio.services;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_MARC_FOR_UPDATE_RECEIVED;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MATCH_PROFILE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import static org.folio.services.ChangeEngineServiceImpl.RECORD_ID_HEADER;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaHeader;

import org.folio.MatchProfile;
import org.folio.rest.jaxrs.model.*;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.afterprocessing.FieldModificationService;
import org.folio.services.validation.JobProfileSnapshotValidationService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.marc.MarcRecordAnalyzer;
import org.folio.dataimport.util.marc.MarcRecordType;
import org.folio.kafka.KafkaConfig;
import org.folio.services.afterprocessing.HrIdFieldService;
import org.folio.services.util.EventHandlingUtil;

@RunWith(MockitoJUnitRunner.class)
public class ChangeEngineServiceImplTest {

  private static final String MARC_HOLDINGS_REC_VALID =
    "00182cx  a22000851  4500001000900000004000800009005001700017008003300034852002900067\u001E10245123\u001E9928371\u001E20170607135730.0\u001E1706072u    8   4001uu   0901128\u001E0 \u001Fbfine\u001FhN7433.3\u001Fi.B87 2014\u001E\u001D";
  private static final String MARC_HOLDINGS_REC_WITHOUT_004 =
    "00162cx  a22000731  4500001000900000005001700009008003300026852002900059\u001E10245123\u001E20170607135730.0\u001E1706072u    8   4001uu   0901128\u001E0 \u001Fbfine\u001FhN7433.3\u001Fi.B87 2014\u001E\u001D";
  private static final String MARC_AUTHORITY_REC_VALID =
    "01016cz  a2200241n  4500001000800000005001700008008004100025010001700066035002300083035002100106040004300127100002200170375000900192377000800201400002400209400003700233400003000270400004400300667004700344670006800391670009400459670022100553\u001E1000649\u001E20171119085041.0\u001E850103n| azannaabn          |b aaa      \u001E  \u001Fan  84234537 \u001E  \u001Fa(OCoLC)oca01249182\u001E  \u001Fa(DLC)n  84234537\u001E  \u001FaDLC\u001Fbeng\u001Ferda\u001FcDLC\u001FdDLC\u001FdCoU\u001FdDLC\u001FdInU\u001E1 \u001FaEimermacher, Karl\u001E  \u001Famale\u001E  \u001Fager\u001E1 \u001FaAĭmermakher, Karl\u001E1 \u001FaАймермахер, Карл\u001E1 \u001FaAĭmermakher, K.\u001Fq(Karl)\u001E1 \u001FaАймермахер, К.\u001Fq(Карл)\u001E  \u001FaNon-Latin script references not evaluated.\u001E  \u001FaSidur, V. Vadim Sidur, 1980?:\u001Fbp. 3 of cover (Karl Eimermacher)\u001E  \u001FaV tiskakh ideologii, 1992:\u001Fbt.p. verso (Karla Aĭmermakhera) colophon (K. Aĭmermakher)\u001E  \u001FaGoogle, 01-31-02\u001Fbruhr-uni-bochum.de/lirsk/whowho.htm (Prof. Dr. Dr. hc. Karl Eimermacher; Geb. 1938; Institutsleiter und Landesbeauftragter für die Hochschulkontakte des Landes NRW zu den europäischen U-Staaten)\u001E\u001D";
  private static final String MARC_BIB_REC_WITHOUT_FF =
    "01119cam a2200349Li 4500001001300000003000600013005001700019008004100036020001800077020001500095035002100110037002200131040002700153043001200180050002700192082001600219090002200235100003300257245002700290264003800317300002300355336002600378337002800404338002700432651006400459945004300523960006200566961001600628980003900644981002300683999006300706\u001Eocn922152790\u001EOCoLC\u001E20150927051630.4\u001E150713s2015    enk           000 f eng d\u001E  \u001Fa9780241146064\u001E  \u001Fa0241146062\u001E  \u001Fa(OCoLC)922152790\u001E  \u001Fa12370236\u001Fbybp\u001F5NU\u001E  \u001FaYDXCP\u001Fbeng\u001Ferda\u001FcYDXCP\u001E  \u001Fae-uk-en\u001E 4\u001FaPR6052.A6488\u001FbN66 2015\u001E04\u001Fa823.914\u001F223\u001E 4\u001Fa823.914\u001FcB2557\u001Fb1\u001E1 \u001FaBarker, Pat,\u001Fd1943-\u001Feauthor.\u001E10\u001FaNoonday /\u001FcPat Barker.\u001E 1\u001FaLondon :\u001FbHamish Hamilton,\u001Fc2015.\u001E  \u001Fa258 pages ;\u001Fc24 cm\u001E  \u001Fatext\u001Fbtxt\u001F2rdacontent\u001E  \u001Faunmediated\u001Fbn\u001F2rdamedia\u001E  \u001Favolume\u001Fbnc\u001F2rdacarrier\u001E 0\u001FaLondon (England)\u001FxHistory\u001FyBombardment, 1940-1941\u001FvFiction.\u001E  \u001Ffh\u001Fg1\u001Fi0000000618391828\u001Flfhgen\u001Fr3\u001Fsv\u001Ft1\u001E  \u001Fap\u001Fda\u001Fgh\u001Fim\u001Fjn\u001Fka\u001Fla\u001Fmo\u001Ftfhgen\u001Fo1\u001Fs15.57\u001Fu7ART\u001Fvukapf\u001FzGBP\u001E  \u001FbGBP\u001Fm633761\u001E  \u001Fa160128\u001Fb1899\u001Fd156\u001Fe1713\u001Ff654270\u001Fg1\u001E  \u001Faukapf\u001Fb7ART\u001Fcfhgen\u001E  \u001Fdm\u001Fea\u001Ffx\u001Fgeng\u001FiTesting with subfield i\u001FsAnd with subfield s\u001E\u001D";
  private static final String MARC_BIB_REC_WITH_FF =
    "00861cam a2200193S1 45 0001000700000002000900007003000400016008004100020035002200061035001300083099001600096245005600112500011600168500019600284600003500480610003400515610003900549999007900588\u001E304162\u001E00320061\u001EPBL\u001E020613n                      000 0 eng u\u001E  \u001Fa(Sirsi)sc99900001\u001E  \u001Fa(Sirsi)1\u001E  \u001FaSC LVF M698\u001E00\u001FaMohler, Harold S. (Lehigh Collection Vertical File)\u001E  \u001FaMaterial on this topic is contained in the Lehigh Collection Vertical File. See Special Collections for access.\u001E  \u001FaContains press releases, versions of resumes, clippings, biographical information. L-in-Life program, and memorial service program -- Documents related Hershey Food Corporation. In two parts.\u001E10\u001FaMohler, Harold S.,\u001Fd1919-1988.\u001E20\u001FaLehigh University.\u001FbTrustees.\u001E20\u001FaLehigh University.\u001FbClass of 1948.\u001Eff\u001Fi29573076-a7ee-462a-8f9b-2659ab7df23c\u001Fs7ca42730-9ba6-4bc8-98d3-f068728504c9\u001E\u001D";

  @Mock
  private JobExecutionSourceChunkDao jobExecutionSourceChunkDao;
  @Mock
  private JobExecutionService jobExecutionService;
  @Mock
  private MarcRecordAnalyzer marcRecordAnalyzer;
  @Mock
  private HrIdFieldService hrIdFieldService;
  @Mock
  private RecordsPublishingService recordsPublishingService;
  @Mock
  private KafkaConfig kafkaConfig;
  @Mock
  private MappingMetadataService mappingMetadataService;
  @Mock
  private JobProfileSnapshotValidationService jobProfileSnapshotValidationService;
  @Mock
  private FieldModificationService fieldModificationService;

  @Captor
  private ArgumentCaptor<List<KafkaHeader>> kafkaHeadersCaptor;

  private final OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(new HashMap<>(), Vertx.vertx());

  @InjectMocks
  private ChangeEngineServiceImpl service;

  @Before
  public void setUp() {
    ReflectionTestUtils.setField(service, "maxDistributionNum", 10);
    ReflectionTestUtils.setField(service, "batchSize", 100);

    when(mappingMetadataService.getMappingMetadataDto(anyString(), any(OkapiConnectionParams.class)))
      .thenReturn(Future.succeededFuture(new MappingMetadataDto()));

    when(jobProfileSnapshotValidationService
      .isJobProfileCompatibleWithRecordType(any(ProfileSnapshotWrapper.class), any(Record.RecordType.class)))
      .thenReturn(true);
  }

  @Test
  public void shouldReturnMarcHoldingsRecord() {
    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(MARC_HOLDINGS_REC_VALID);
    JobExecution jobExecution = getTestJobExecution();

    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.HOLDING);
    when(jobExecutionSourceChunkDao.getById(any(), any()))
      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));

    Future<List<Record>> serviceFuture = executeWithKafkaMock(rawRecordsDto, jobExecution, Future.succeededFuture(true));

    var actual = serviceFuture.result();
    assertThat(actual, hasSize(1));
    assertThat(actual.get(0).getRecordType(), equalTo(Record.RecordType.MARC_HOLDING));
    assertThat(actual.get(0).getErrorRecord(), nullValue());
  }

  @Test
  public void shouldReturnMarcAuthorityRecordWithAuthorityId() {
    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(MARC_AUTHORITY_REC_VALID);
    JobExecution jobExecution = getTestJobExecution();

    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.AUTHORITY);
    when(jobExecutionSourceChunkDao.getById(any(), any()))
      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));

    Future<List<Record>> serviceFuture = executeWithKafkaMock(rawRecordsDto, jobExecution, Future.succeededFuture(true));

    var actual = serviceFuture.result();
    assertThat(actual, hasSize(1));
    assertThat(actual.get(0).getRecordType(), equalTo(Record.RecordType.MARC_AUTHORITY));
    assertThat(actual.get(0).getErrorRecord(), nullValue());
    assertThat(actual.get(0).getExternalIdsHolder().getAuthorityId(), notNullValue());
    assertThat(actual.get(0).getExternalIdsHolder().getAuthorityHrid(), notNullValue());
  }

  @Test
  public void shouldReturnMarcHoldingsRecordWhenProfileHasUpdateAction() {
    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(MARC_HOLDINGS_REC_VALID);
    JobExecution jobExecution = getTestJobExecution();
    jobExecution.setJobProfileSnapshotWrapper(new ProfileSnapshotWrapper()
      .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
        .withContentType(ACTION_PROFILE)
        .withContent(new JsonObject(Json.encode(new ActionProfile()
          .withAction(ActionProfile.Action.UPDATE)
          .withFolioRecord(ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC))).getMap())
      ))
    );

    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.HOLDING);
    when(jobExecutionSourceChunkDao.getById(any(), any()))
      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));
    when(recordsPublishingService.sendEventsWithRecords(any(), any(), any(), any()))
      .thenReturn(Future.succeededFuture(true));

    Future<List<Record>> serviceFuture = executeWithKafkaMock(rawRecordsDto, jobExecution, Future.succeededFuture(true));

    var actual = serviceFuture.result();
    assertThat(actual, hasSize(1));
    assertThat(actual.get(0).getRecordType(), equalTo(Record.RecordType.MARC_HOLDING));
    assertThat(actual.get(0).getErrorRecord(), nullValue());
  }

  @Test
  public void shouldReturnMarcAuthorityRecordWhenProfileHasDeleteAction() {
    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(MARC_AUTHORITY_REC_VALID);
    JobExecution jobExecution = getTestJobExecution();
    jobExecution.setJobProfileSnapshotWrapper(new ProfileSnapshotWrapper()
      .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
        .withContentType(ACTION_PROFILE)
        .withContent(new JsonObject(Json.encode(new ActionProfile()
          .withAction(ActionProfile.Action.DELETE)
          .withFolioRecord(ActionProfile.FolioRecord.MARC_AUTHORITY))).getMap())
      ))
    );

    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.AUTHORITY);
    when(jobExecutionSourceChunkDao.getById(any(), any()))
      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));
    when(recordsPublishingService.sendEventsWithRecords(any(), any(), any(), any()))
      .thenReturn(Future.succeededFuture(true));

    Future<List<Record>> serviceFuture = executeWithKafkaMock(rawRecordsDto, jobExecution, Future.succeededFuture(true));

    var actual = serviceFuture.result();
    assertThat(actual, hasSize(1));
    assertThat(actual.get(0).getRecordType(), equalTo(Record.RecordType.MARC_AUTHORITY));
    assertThat(actual.get(0).getErrorRecord(), nullValue());
  }

  @Test
  public void shouldNotReturnMarcHoldingsRecordWhen004FieldIsMissing() {
    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(MARC_HOLDINGS_REC_WITHOUT_004);
    JobExecution jobExecution = getTestJobExecution();

    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.HOLDING);
    when(jobExecutionSourceChunkDao.getById(any(), any()))
      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));

    Future<List<Record>> serviceFuture = executeWithKafkaMock(rawRecordsDto, jobExecution, Future.succeededFuture(true));

    var actual = serviceFuture.result();
    assertThat(actual, hasSize(0));
  }

  @Test
  public void shouldFillRecordIdHeaderForMarkRecordWhen004FieldIsMissing() {
    var rawRecordsDto = getTestRawRecordsDto(MARC_HOLDINGS_REC_WITHOUT_004);
    var jobExecution = getTestJobExecution();

    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.HOLDING);
    when(jobExecutionSourceChunkDao.getById(any(), any()))
      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));

    try (var mockedStatic = Mockito.mockStatic(EventHandlingUtil.class)) {
      mockedStatic.when(() -> EventHandlingUtil.sendEventToKafka(any(), any(), any(), kafkaHeadersCaptor.capture(), any(), any()))
        .thenReturn(Future.succeededFuture(true));
      service.parseRawRecordsChunkForJobExecution(rawRecordsDto, jobExecution, "1", false, okapiConnectionParams).result();
    }

    var optionalRecordIdHeader = kafkaHeadersCaptor.getValue().stream()
      .filter(kafkaHeader -> kafkaHeader.key().equals(RECORD_ID_HEADER))
      .findFirst();

    assertTrue(optionalRecordIdHeader.isPresent());
  }

  @Test
  public void shouldFailedWhenKafkaFailedToSendEvent() {
    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(MARC_HOLDINGS_REC_VALID);
    JobExecution jobExecution = getTestJobExecution();

    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.HOLDING);
    when(jobExecutionSourceChunkDao.getById(any(), any()))
      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));
    when(jobExecutionService.updateJobExecutionStatus(any(), any(), any())).thenReturn(Future.succeededFuture(jobExecution));

    Future<List<Record>> serviceFuture = executeWithKafkaMock(rawRecordsDto, jobExecution, Future.failedFuture("Failed"));

    assertTrue(serviceFuture.failed());
  }

  @Test
  public void shouldFailedWhenKafkaFailedToSendEventAndFailedToUpdateJobExecutionStatus() {
    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(MARC_HOLDINGS_REC_VALID);
    JobExecution jobExecution = getTestJobExecution();

    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.HOLDING);
    when(jobExecutionSourceChunkDao.getById(any(), any()))
      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));
    when(jobExecutionService.updateJobExecutionStatus(any(), any(), any())).thenReturn(Future.failedFuture("Failed"));

    Future<List<Record>> serviceFuture = executeWithKafkaMock(rawRecordsDto, jobExecution, Future.failedFuture("Failed"));

    assertTrue(serviceFuture.failed());
  }

  @Test
  public void shouldReturnMarcBibRecord() {
    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(MARC_BIB_REC_WITHOUT_FF);
    JobExecution jobExecution = getTestJobExecution();

    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.BIB);
    when(jobExecutionSourceChunkDao.getById(any(), any()))
      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));

    Future<List<Record>> serviceFuture = executeWithKafkaMock(rawRecordsDto, jobExecution, Future.succeededFuture(true));

    var actual = serviceFuture.result();
    assertThat(actual, hasSize(1));
    assertThat(actual.get(0).getRecordType(), equalTo(Record.RecordType.MARC_BIB));
    assertThat(actual.get(0).getErrorRecord(), nullValue());
  }

  @Test
  public void shouldReturnMarcBibRecordWith999ByAcceptInstanceId() {

    boolean acceptInstanceId = true;

    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(MARC_BIB_REC_WITH_FF);
    JobExecution jobExecution = new JobExecution()
      .withId(UUID.randomUUID().toString())
      .withUserId(UUID.randomUUID().toString())
      .withJobProfileSnapshotWrapper(constructCreateInstanceSnapshotWrapper())
      .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString())
        .withName("test").withDataType(JobProfileInfo.DataType.MARC));

    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.BIB);
    when(jobExecutionSourceChunkDao.getById(any(), any()))
      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));

    Future<List<Record>> serviceFuture =
      executeWithKafkaMock(rawRecordsDto, jobExecution, Future.succeededFuture(true), acceptInstanceId);

    var actual = serviceFuture.result();
    assertThat(actual, hasSize(1));
    assertThat(actual.get(0).getRecordType(), equalTo(Record.RecordType.MARC_BIB));
    assertThat(actual.get(0).getErrorRecord(), nullValue());
    assertThat(actual.get(0).getMatchedId(), equalTo("7ca42730-9ba6-4bc8-98d3-f068728504c9"));
    assertThat(actual.get(0).getExternalIdsHolder().getInstanceId(), equalTo("29573076-a7ee-462a-8f9b-2659ab7df23c"));
  }

  @Test
  public void shouldReturnMarcBibRecordWithIds() {
    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(MARC_BIB_REC_WITH_FF);
    JobExecution jobExecution = getTestJobExecution();

    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.BIB);
    when(jobExecutionSourceChunkDao.getById(any(), any()))
      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));

    Future<List<Record>> serviceFuture = executeWithKafkaMock(rawRecordsDto, jobExecution, Future.succeededFuture(true));

    var actual = serviceFuture.result();
    assertThat(actual, hasSize(1));
    assertThat(actual.get(0).getRecordType(), equalTo(Record.RecordType.MARC_BIB));
    assertThat(actual.get(0).getErrorRecord(), nullValue());
    assertThat(actual.get(0).getMatchedId(), equalTo("7ca42730-9ba6-4bc8-98d3-f068728504c9"));
    assertThat(actual.get(0).getExternalIdsHolder().getInstanceId(), equalTo("29573076-a7ee-462a-8f9b-2659ab7df23c"));
  }

  @Test
  public void shouldOnlyUpdateIfOnlyCreateHoldings() {
    String rawMarc = "00182cc  a22000851  4500001000900000004000800009005001700017008003300034852002900067\u001E10245123\u001E9928371\u001E20170607135730.0\u001E1706072u    8   4001uu   0901128\u001E0 \u001Fbfine\u001FhN7433.3\u001Fi.B87 2014\u001E\u001D";

    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(rawMarc);
    JobExecution jobExecution = new JobExecution()
      .withId(UUID.randomUUID().toString())
      .withUserId(UUID.randomUUID().toString())
      .withJobProfileSnapshotWrapper(constructCreateMarcHoldingsSnapshotWrapper())
      .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString())
        .withName("test").withDataType(JobProfileInfo.DataType.MARC));

    mockServicesForParseRawRecordsChunkForJobExecution();

    try (var mockedStatic = Mockito.mockStatic(EventHandlingUtil.class)) {
      mockedStatic.when(() -> EventHandlingUtil.sendEventToKafka(any(), any(), any(), kafkaHeadersCaptor.capture(), any(), any()))
        .thenReturn(Future.succeededFuture(true));
      service.parseRawRecordsChunkForJobExecution(rawRecordsDto, jobExecution, "1", false, okapiConnectionParams).result();
    }

    verify(recordsPublishingService).sendEventsWithRecords(any(), eq(jobExecution.getId()), any(), eq(DI_MARC_FOR_UPDATE_RECEIVED.value()));
  }

  @Test
  public void shouldOnlyUpdateIfOnlyUpdateItem() {
    String rawMarc = "00182cc  a22000851  4500001000900000004000800009005001700017008003300034852002900067\u001E10245123\u001E9928371\u001E20170607135730.0\u001E1706072u    8   4001uu   0901128\u001E0 \u001Fbfine\u001FhN7433.3\u001Fi.B87 2014\u001E\u001D";

    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(rawMarc);
    JobExecution jobExecution = new JobExecution()
      .withId(UUID.randomUUID().toString())
      .withUserId(UUID.randomUUID().toString())
      .withJobProfileSnapshotWrapper(constructUpdateMarcItemSnapshotWrapper())
      .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString())
        .withName("test").withDataType(JobProfileInfo.DataType.MARC));

    mockServicesForParseRawRecordsChunkForJobExecution();

    try (var mockedStatic = Mockito.mockStatic(EventHandlingUtil.class)) {
      mockedStatic.when(() -> EventHandlingUtil.sendEventToKafka(any(), any(), any(), kafkaHeadersCaptor.capture(), any(), any()))
        .thenReturn(Future.succeededFuture(true));
      service.parseRawRecordsChunkForJobExecution(rawRecordsDto, jobExecution, "1", false, okapiConnectionParams).result();
    }

    verify(recordsPublishingService).sendEventsWithRecords(any(), eq(jobExecution.getId()), any(), eq(DI_MARC_FOR_UPDATE_RECEIVED.value()));
  }

  @Test
  public void shouldNotUpdateIfRecordTypeIsNotMarcBib() {
    String rawMarc = "00182uu  a22000851  4500001000900000004000800009005001700017008003300034852002900067\u001E10245123\u001E9928371\u001E20170607135730.0\u001E1706072u    8   4001uu   0901128\u001E0 \u001Fbfine\u001FhN7433.3\u001Fi.B87 2014\u001E\u001D";

    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(rawMarc);
    JobExecution jobExecution = new JobExecution()
      .withId(UUID.randomUUID().toString())
      .withUserId(UUID.randomUUID().toString())
      .withJobProfileSnapshotWrapper(constructCreateMarcHoldingsSnapshotWrapper())
      .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString())
        .withName("test").withDataType(JobProfileInfo.DataType.MARC));

    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.HOLDING);
    when(jobExecutionSourceChunkDao.getById(any(), any()))
      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));


    try (var mockedStatic = Mockito.mockStatic(EventHandlingUtil.class)) {
      mockedStatic.when(() -> EventHandlingUtil.sendEventToKafka(any(), any(), any(), kafkaHeadersCaptor.capture(), any(), any()))
        .thenReturn(Future.succeededFuture(true));
      service.parseRawRecordsChunkForJobExecution(rawRecordsDto, jobExecution, "1", false, okapiConnectionParams).result();
    }

    verify(recordsPublishingService, never()).sendEventsWithRecords(any(), any(), any(), any());
  }

  @Test
  public void shouldNotUpdateIfCreateInstanceActionExist() {
    String rawMarc = "00182cc  a22000851  4500001000900000004000800009005001700017008003300034852002900067\u001E10245123\u001E9928371\u001E20170607135730.0\u001E1706072u    8   4001uu   0901128\u001E0 \u001Fbfine\u001FhN7433.3\u001Fi.B87 2014\u001E\u001D";

    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(rawMarc);
    JobExecution jobExecution = new JobExecution()
      .withId(UUID.randomUUID().toString())
      .withUserId(UUID.randomUUID().toString())
      .withJobProfileSnapshotWrapper(constructCreateMarcHoldingsAndInstanceSnapshotWrapper())
      .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString())
        .withName("test").withDataType(JobProfileInfo.DataType.MARC));

    mockServicesForParseRawRecordsChunkForJobExecution();

    try (var mockedStatic = Mockito.mockStatic(EventHandlingUtil.class)) {
      mockedStatic.when(() -> EventHandlingUtil.sendEventToKafka(any(), any(), any(), kafkaHeadersCaptor.capture(), any(), any()))
        .thenReturn(Future.succeededFuture(true));
      service.parseRawRecordsChunkForJobExecution(rawRecordsDto, jobExecution, "1", false, okapiConnectionParams).result();
    }

    verify(recordsPublishingService, never()).sendEventsWithRecords(any(), any(), any(), any());
  }

  @Test
  public void shouldNotUpdateIfNoParsedRecords() {
    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(MARC_HOLDINGS_REC_WITHOUT_004);
    JobExecution jobExecution = new JobExecution()
      .withId(UUID.randomUUID().toString())
      .withUserId(UUID.randomUUID().toString())
      .withJobProfileSnapshotWrapper(constructUpdateMarcItemSnapshotWrapper())
      .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString())
        .withName("test").withDataType(JobProfileInfo.DataType.MARC));

    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.HOLDING);
    when(jobExecutionSourceChunkDao.getById(any(), any()))
      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));

    try (var mockedStatic = Mockito.mockStatic(EventHandlingUtil.class)) {
      mockedStatic.when(() -> EventHandlingUtil.sendEventToKafka(any(), any(), any(), kafkaHeadersCaptor.capture(), any(), any()))
        .thenReturn(Future.succeededFuture(true));
      service.parseRawRecordsChunkForJobExecution(rawRecordsDto, jobExecution, "1", false, okapiConnectionParams).result();
    }

    verify(recordsPublishingService, never()).sendEventsWithRecords(any(), any(), any(), any());
  }

  @Test
  public void shouldRemoveSubfield9WhenSpecified() {
    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(MARC_AUTHORITY_REC_VALID);
    JobExecution jobExecution = getTestJobExecution();
    jobExecution.setJobProfileSnapshotWrapper(new ProfileSnapshotWrapper()
      .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
        .withContentType(MATCH_PROFILE)
        .withContent(new JsonObject(Json.encode(new MatchProfile())).getMap())
        .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
          .withContentType(ACTION_PROFILE)
          .withContent(new JsonObject(Json.encode(new ActionProfile()
            .withAction(ActionProfile.Action.DELETE)
            .withFolioRecord(ActionProfile.FolioRecord.MARC_AUTHORITY)
            .withRemove9Subfields(true))).getMap())
        ))
      ))
    );

    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.AUTHORITY);
    when(jobExecutionSourceChunkDao.getById(any(), any()))
      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));
    when(recordsPublishingService.sendEventsWithRecords(any(), any(), any(), any()))
      .thenReturn(Future.succeededFuture(true));
    doAnswer(invocationOnMock -> Future.succeededFuture(invocationOnMock.getArgument(1)))
      .when(fieldModificationService).remove9Subfields(any(), any(), any());

    Future<List<Record>> serviceFuture = executeWithKafkaMock(rawRecordsDto, jobExecution, Future.succeededFuture(true));

    var actual = serviceFuture.result();
    assertThat(actual, hasSize(1));
    assertThat(actual.get(0).getRecordType(), equalTo(Record.RecordType.MARC_AUTHORITY));
    assertThat(actual.get(0).getErrorRecord(), nullValue());
    verify(fieldModificationService).remove9Subfields(eq(jobExecution.getId()), any(), any());
  }

  @Test
  public void shouldNotRemoveSubfield9WhenNotSpecified() {
    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(MARC_AUTHORITY_REC_VALID);
    JobExecution jobExecution = getTestJobExecution();
    jobExecution.setJobProfileSnapshotWrapper(new ProfileSnapshotWrapper()
      .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
        .withContentType(ACTION_PROFILE)
        .withContent(new JsonObject(Json.encode(new ActionProfile()
          .withAction(ActionProfile.Action.DELETE)
          .withFolioRecord(ActionProfile.FolioRecord.MARC_AUTHORITY)
          .withRemove9Subfields(false))).getMap())
      ))
    );

    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.AUTHORITY);
    when(jobExecutionSourceChunkDao.getById(any(), any()))
      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));
    when(recordsPublishingService.sendEventsWithRecords(any(), any(), any(), any()))
      .thenReturn(Future.succeededFuture(true));

    Future<List<Record>> serviceFuture = executeWithKafkaMock(rawRecordsDto, jobExecution, Future.succeededFuture(true));

    var actual = serviceFuture.result();
    assertThat(actual, hasSize(1));
    assertThat(actual.get(0).getRecordType(), equalTo(Record.RecordType.MARC_AUTHORITY));
    assertThat(actual.get(0).getErrorRecord(), nullValue());
    verifyNoInteractions(fieldModificationService);
  }

  private void mockServicesForParseRawRecordsChunkForJobExecution() {
    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.BIB);
    when(recordsPublishingService.sendEventsWithRecords(any(), any(), any(), any())).thenReturn(Future.succeededFuture(true));
    when(jobExecutionSourceChunkDao.getById(any(), any()))
      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));

  }

  ProfileSnapshotWrapper constructCreateInstanceSnapshotWrapper() {
    return new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withContent(new JsonObject())
      .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
        .withProfileId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(new JsonObject(Json.encode(new ActionProfile()
          .withId(UUID.randomUUID().toString())
          .withName("Create Instance")
          .withAction(ActionProfile.Action.CREATE)
          .withFolioRecord(ActionProfile.FolioRecord.INSTANCE))).getMap())
      ));
  };

  private ProfileSnapshotWrapper constructCreateMarcHoldingsAndInstanceSnapshotWrapper() {
    return new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withContent(new JsonObject())
      .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
          .withProfileId(UUID.randomUUID().toString())
          .withContentType(ACTION_PROFILE)
          .withContent(new JsonObject(Json.encode(new ActionProfile()
            .withId(UUID.randomUUID().toString())
            .withName("Create Instance")
            .withAction(ActionProfile.Action.CREATE)
            .withFolioRecord(ActionProfile.FolioRecord.INSTANCE))).getMap())
        , new ProfileSnapshotWrapper()
          .withProfileId(UUID.randomUUID().toString())
          .withContentType(ACTION_PROFILE)
          .withContent(new JsonObject(Json.encode(new ActionProfile()
            .withId(UUID.randomUUID().toString())
            .withName("Create MARC-Holdings ")
            .withAction(ActionProfile.Action.CREATE)
            .withFolioRecord(ActionProfile.FolioRecord.HOLDINGS))).getMap())
          .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
            .withProfileId(UUID.randomUUID().toString())
            .withContentType(MAPPING_PROFILE)
            .withContent(new JsonObject(Json.encode(new MappingProfile()
              .withId(UUID.randomUUID().toString())
              .withName("Create MARC-Holdings ")
              .withIncomingRecordType(EntityType.MARC_HOLDINGS)
              .withExistingRecordType(EntityType.HOLDINGS))).getMap()
            )))));
  }

  ProfileSnapshotWrapper constructCreateMarcHoldingsSnapshotWrapper() {
    return new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withContent(new JsonObject())
      .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
        .withProfileId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(new JsonObject(Json.encode(new ActionProfile()
          .withId(UUID.randomUUID().toString())
          .withName("Create MARC-Holdings ")
          .withAction(ActionProfile.Action.CREATE)
          .withFolioRecord(ActionProfile.FolioRecord.HOLDINGS))).getMap())
        .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
          .withProfileId(UUID.randomUUID().toString())
          .withContentType(MAPPING_PROFILE)
          .withContent(new JsonObject(Json.encode(new MappingProfile()
            .withId(UUID.randomUUID().toString())
            .withName("Create MARC-Holdings ")
            .withIncomingRecordType(EntityType.MARC_HOLDINGS)
            .withExistingRecordType(EntityType.HOLDINGS))).getMap()
          )))));
  }

  ProfileSnapshotWrapper constructUpdateMarcItemSnapshotWrapper() {
    return new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withContentType(JOB_PROFILE)
      .withContent(new JsonObject())
      .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
        .withProfileId(UUID.randomUUID().toString())
        .withContentType(ACTION_PROFILE)
        .withContent(new JsonObject(Json.encode(new ActionProfile()
          .withId(UUID.randomUUID().toString())
          .withName("Create MARC-Holdings ")
          .withAction(ActionProfile.Action.UPDATE)
          .withFolioRecord(ActionProfile.FolioRecord.ITEM))).getMap())
        .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper()
          .withProfileId(UUID.randomUUID().toString())
          .withContentType(MAPPING_PROFILE)
          .withContent(new JsonObject(Json.encode(new MappingProfile()
            .withId(UUID.randomUUID().toString())
            .withName("Create MARC-Holdings ")
            .withIncomingRecordType(EntityType.MARC_HOLDINGS)
            .withExistingRecordType(EntityType.ITEM))).getMap()
          )))));
  }

  private RawRecordsDto getTestRawRecordsDto(String marcHoldingsRecValid) {
    return new RawRecordsDto().withId(UUID.randomUUID().toString())
      .withRecordsMetadata(new RecordsMetadata().withContentType(RecordsMetadata.ContentType.MARC_RAW))
      .withInitialRecords(Collections.singletonList(new InitialRecord().withRecord(marcHoldingsRecValid)));
  }

  private JobExecution getTestJobExecution() {
    return new JobExecution()
      .withId(UUID.randomUUID().toString())
      .withUserId(UUID.randomUUID().toString())
      .withJobProfileSnapshotWrapper(new ProfileSnapshotWrapper())
      .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString())
        .withName("test").withDataType(JobProfileInfo.DataType.MARC));
  }

  private Future<List<Record>> executeWithKafkaMock(RawRecordsDto rawRecordsDto, JobExecution jobExecution,
                                                    Future<Boolean> eventSentResult) {
    return executeWithKafkaMock(rawRecordsDto, jobExecution, eventSentResult, false);
  }

  private Future<List<Record>> executeWithKafkaMock(RawRecordsDto rawRecordsDto, JobExecution jobExecution,
                                                    Future<Boolean> eventSentResult, boolean acceptInstanceId) {
    try (var mockedStatic = Mockito.mockStatic(EventHandlingUtil.class)) {
      mockedStatic.when(() -> EventHandlingUtil.sendEventToKafka(any(), any(), any(), anyList(), any(), any()))
        .thenReturn(eventSentResult);
      return service.parseRawRecordsChunkForJobExecution(rawRecordsDto, jobExecution, "1", acceptInstanceId, okapiConnectionParams);
    }
  }
}
