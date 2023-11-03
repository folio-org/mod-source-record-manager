package org.folio.services;

import static com.github.tomakehurst.wiremock.client.WireMock.created;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.jaxrs.model.StatusDto.Status.PARSING_IN_PROGRESS;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import org.folio.Record;
import org.folio.TestUtil;
import org.folio.dao.JobExecutionDaoImpl;
import org.folio.dao.JobExecutionProgressDaoImpl;
import org.folio.dao.JobExecutionSourceChunkDaoImpl;
import org.folio.dao.JournalRecordDaoImpl;
import org.folio.dao.IncomingRecordDaoImpl;
import org.folio.dao.MappingParamsSnapshotDaoImpl;
import org.folio.dao.MappingRuleDaoImpl;
import org.folio.dao.MappingRulesSnapshotDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.marc.MarcRecordAnalyzer;
import org.folio.kafka.KafkaConfig;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.JobProfileInfo.DataType;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.RecordsMetadata;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.afterprocessing.FieldModificationServiceImpl;
import org.folio.services.afterprocessing.HrIdFieldServiceImpl;
import org.folio.services.mappers.processor.MappingParametersProvider;
import org.folio.services.progress.JobExecutionProgressServiceImpl;
import org.folio.services.validation.JobProfileSnapshotValidationServiceImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.test.util.ReflectionTestUtils;

@RunWith(VertxUnitRunner.class)
public class EventDrivenChunkProcessingServiceImplTest extends AbstractRestTest {

  private static final String CORRECT_RAW_RECORD = "01240cas a2200397   450000100070000000500170000700800410002401000170006502200140008203500260009603500220012203500110014403500190015504000440017405000150021808200110023322200420024424500430028626000470032926500380037630000150041431000220042932100250045136200230047657000290049965000330052865000450056165500420060670000450064885300180069386300230071190200160073490500210075094800370077195000340080836683220141106221425.0750907c19509999enkqr p       0   a0eng d  a   58020553   a0022-0469  a(CStRLIN)NYCX1604275S  a(NIC)notisABP6388  a366832  a(OCoLC)1604275  dCtYdMBTIdCtYdMBTIdNICdCStRLINdNIC0 aBR140b.J6  a270.0504aThe Journal of ecclesiastical history04aThe Journal of ecclesiastical history.  aLondon,bCambridge University Press [etc.]  a32 East 57th St., New York, 10022  av.b25 cm.  aQuarterly,b1970-  aSemiannual,b1950-690 av. 1-   Apr. 1950-  aEditor:   C. W. Dugmore. 0aChurch historyxPeriodicals. 7aChurch history2fast0(OCoLC)fst00860740 7aPeriodicals2fast0(OCoLC)fst014116411 aDugmore, C. W.q(Clifford William),eed.0381av.i(year)4081a1-49i1950-1998  apfndbLintz  a19890510120000.02 a20141106bmdbatcheltsxaddfast  lOLINaBR140b.J86h01/01/01 N01542ccm a2200361   ";
  private static final String RAW_RECORD_RESULTING_IN_PARSING_ERROR = "01247nam  2200313zu 450000100110000000300080001100500170001905\u001F222\u001E1 \u001FaAriáes, Philippe.\u001E10\u001FaWestern attitudes toward death\u001Fh[electronic resource] :\u001Fbfrom the Middle Ages to the present /\u001Fcby Philippe Ariáes ; translated by Patricia M. Ranum.\u001E  \u001FaJohn Hopkins Paperbacks ed.\u001E  \u001FaBaltimore :\u001FbJohns Hopkins University Press,\u001Fc1975.\u001E  \u001Fa1 online resource.\u001E1 \u001FaThe Johns Hopkins symposia in comparative history ;\u001Fv4th\u001E  \u001FaDescription based on online resource; title from digital title page (viewed on Mar. 7, 2013).\u001E 0\u001FaDeath.\u001E2 \u001FaEbrary.\u001E 0\u001FaJohns Hopkins symposia in comparative history ;\u001Fv4th.\u001E40\u001FzConnect to e-book on Ebrary\u001Fuhttp://gateway.library.qut.edu.au/login?url=http://site.ebrary.com/lib/qut/docDetail.action?docID=10635130\u001E  \u001Fa.o1346565x\u001E  \u001Fa130307\u001Fb2095\u001Fe2095\u001Ff243966\u001Fg1\u001E  \u001FbOM\u001Fcnlnet\u001E\u001D\n";
  private static final String RULES_PATH = "src/test/resources/org/folio/services/marc_bib_rules.json";
  private static final String KAFKA_ENV_ID = "test-env";

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  private Vertx vertx = Vertx.vertx();
  @Spy
  private PostgresClientFactory postgresClientFactory = new PostgresClientFactory(vertx);
  @Spy
  @InjectMocks
  private JobExecutionDaoImpl jobExecutionDao;
  @Spy
  @InjectMocks
  private MappingRuleServiceImpl mappingRuleService;
  @Spy
  @InjectMocks
  private MappingRuleDaoImpl mappingRuleDao;
  @Spy
  @InjectMocks
  private MappingParametersProvider mappingParametersProvider;
  @Spy
  @InjectMocks
  private JobExecutionSourceChunkDaoImpl jobExecutionSourceChunkDao;
  @InjectMocks
  @Spy
  private JobExecutionServiceImpl jobExecutionService;
  @InjectMocks
  @Spy
  private IncomingRecordServiceImpl incomingRecordService;
  @InjectMocks
  @Spy
  private IncomingRecordDaoImpl incomingRecordDao;
  @InjectMocks
  @Spy
  private JournalRecordServiceImpl journalRecordService;
  @InjectMocks
  @Spy
  private MarcRecordAnalyzer marcRecordAnalyzer;
  @InjectMocks
  @Spy
  private JobExecutionProgressDaoImpl jobExecutionProgressDao;
  @Spy
  @InjectMocks
  private JobExecutionProgressServiceImpl jobExecutionProgressService;
  @Spy
  private HrIdFieldServiceImpl hrIdFieldService;
  @Spy
  @InjectMocks
  private JournalRecordDaoImpl journalRecordDao;
  @Spy
  @InjectMocks
  private MappingRulesSnapshotDaoImpl mappingRulesSnapshotDao;
  @Spy
  @InjectMocks
  private MappingParamsSnapshotDaoImpl mappingParamsSnapshotDao;
  @Spy
  private RecordsPublishingService recordsPublishingService;
  @Spy
  @InjectMocks
  private FieldModificationServiceImpl fieldModificationService;

  private AutoCloseable mocks;
  private KafkaConfig kafkaConfig;
  private MappingRuleCache mappingRuleCache;
  private ChangeEngineService changeEngineService;
  private ChunkProcessingService chunkProcessingService;
  private OkapiConnectionParams params;
  private MappingMetadataService mappingMetadataService;
  private InitJobExecutionsRqDto initJobExecutionsRqDto = new InitJobExecutionsRqDto()
    .withFiles(Collections.singletonList(new File().withName("importBib1.bib")))
    .withSourceType(InitJobExecutionsRqDto.SourceType.FILES)
    .withUserId(okapiUserIdHeader);

  private RawRecordsDto rawRecordsDto = new RawRecordsDto()
    .withId(UUID.randomUUID().toString())
    .withRecordsMetadata(new RecordsMetadata()
      .withLast(false)
      .withCounter(15)
      .withTotal(15)
      .withContentType(RecordsMetadata.ContentType.MARC_RAW))
    .withInitialRecords(Collections.singletonList(new InitialRecord().withRecord(CORRECT_RAW_RECORD)));

  private JobProfileInfo jobProfileInfo = new JobProfileInfo()
    .withName("MARC records")
    .withId(jobProfile.getId())
    .withDataType(DataType.MARC);

  @Before
  public void setUp() throws IOException {
    String rules = TestUtil.readFileFromPath(RULES_PATH);
    this.mocks = MockitoAnnotations.openMocks(this);
    String[] hostAndPort = kafkaCluster.getBrokerList().split(":");
    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(hostAndPort[0])
      .kafkaPort(hostAndPort[1])
      .envId(KAFKA_ENV_ID)
      .build();

    mappingRuleDao = when(mock(MappingRuleDaoImpl.class).get(any(Record.RecordType.class), anyString())).thenReturn(Future.succeededFuture(Optional.of(new JsonObject(rules)))).getMock();
    marcRecordAnalyzer = new MarcRecordAnalyzer();
    mappingRuleCache = new MappingRuleCache(mappingRuleDao, vertx);
    mappingRuleService = new MappingRuleServiceImpl(mappingRuleDao, mappingRuleCache);
    mappingParametersProvider = when(mock(MappingParametersProvider.class).get(anyString(), any(OkapiConnectionParams.class))).thenReturn(Future.succeededFuture(new MappingParameters())).getMock();

    mappingMetadataService = new MappingMetadataServiceImpl(mappingParametersProvider, mappingRuleService, mappingRulesSnapshotDao, mappingParamsSnapshotDao);
    JobProfileSnapshotValidationServiceImpl jobProfileSnapshotValidationService = new JobProfileSnapshotValidationServiceImpl();
    changeEngineService = new ChangeEngineServiceImpl(jobExecutionSourceChunkDao, jobExecutionService, marcRecordAnalyzer,
      hrIdFieldService, recordsPublishingService, mappingMetadataService, jobProfileSnapshotValidationService, kafkaConfig,
      fieldModificationService, incomingRecordService, journalRecordService);
    ReflectionTestUtils.setField(changeEngineService, "maxDistributionNum", 10);
    ReflectionTestUtils.setField(changeEngineService, "batchSize", 100);
    chunkProcessingService = new EventDrivenChunkProcessingServiceImpl(jobExecutionSourceChunkDao, jobExecutionService, changeEngineService, jobExecutionProgressService);

    HashMap<String, String> headers = new HashMap<>();
    headers.put(OKAPI_URL_HEADER, "http://localhost:" + snapshotMockServer.port());
    headers.put(OKAPI_TENANT_HEADER, TENANT_ID);
    headers.put(OKAPI_TOKEN_HEADER, "token");
    params = new OkapiConnectionParams(headers, vertx);

    WireMock.stubFor(post(RECORDS_SERVICE_URL)
      .willReturn(created().withTransformers(RequestToResponseTransformer.NAME)));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern("/data-import-profiles/jobProfiles/" + ".*"), true))
      .willReturn(ok().withBody(JsonObject.mapFrom(jobProfile).encode())));
  }

  @After
  public void tearDown() throws Exception {
    mocks.close();
  }

  @Test
  public void shouldProcessChunkOfRawRecords(TestContext context) {
    Async async = context.async();
    Future<Boolean> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> jobExecutionService.setJobProfileToJobExecution(initJobExecutionsRsDto.getParentJobExecutionId(), jobProfileInfo, params))
      .compose(jobExecution -> chunkProcessingService.processChunk(rawRecordsDto, jobExecution.getId(), false, params));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      ArgumentCaptor<StatusDto> statusCaptor = ArgumentCaptor.forClass(StatusDto.class);
      ArgumentCaptor<String> jobExecutionIdCaptor = ArgumentCaptor.forClass(String.class);
      Mockito.verify(jobExecutionService).updateJobExecutionStatus(jobExecutionIdCaptor.capture(), statusCaptor.capture(), isA(OkapiConnectionParams.class));
      Mockito.verify(jobExecutionProgressService).initializeJobExecutionProgress(anyString(), eq(rawRecordsDto.getRecordsMetadata().getTotal()), eq(TENANT_ID));
      context.assertTrue(PARSING_IN_PROGRESS.equals(statusCaptor.getValue().getStatus()));

      mappingMetadataService.getMappingMetadataDto(jobExecutionIdCaptor.getValue(), params)
        .onComplete(mappingMetadataDtoAsyncResult -> {
          context.assertTrue(mappingMetadataDtoAsyncResult.succeeded());
          context.assertTrue(mappingMetadataDtoAsyncResult.result().getJobExecutionId().equals(jobExecutionIdCaptor.getValue()));
        });
      async.complete();
    });
  }

  @Test
  public void shouldReturnSucceededFutureWhenFailedPostSnapshotsToRecordsStorage(TestContext context) {
    Async async = context.async();

    WireMock.stubFor(put(SNAPSHOT_SERVICE_URL)
      .willReturn(WireMock.serverError()));

    Future<Boolean> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> jobExecutionService.setJobProfileToJobExecution(initJobExecutionsRsDto.getParentJobExecutionId(), jobProfileInfo, params))
      .compose(jobExecution -> chunkProcessingService.processChunk(rawRecordsDto, jobExecution.getId(), false, params));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      ArgumentCaptor<StatusDto> captor = ArgumentCaptor.forClass(StatusDto.class);
      Mockito.verify(jobExecutionService, times(1)).updateJobExecutionStatus(anyString(), captor.capture(), isA(OkapiConnectionParams.class));
      verify(1, postRequestedFor(urlEqualTo(SNAPSHOT_SERVICE_URL)));
      async.complete();
    });
  }

  @Test
  public void shouldProcessErrorRawRecord(TestContext context) {
    Async async = context.async();

    RawRecordsDto rawRecordsDto = new RawRecordsDto()
      .withRecordsMetadata(new RecordsMetadata()
        .withLast(false)
        .withCounter(1)
        .withContentType(RecordsMetadata.ContentType.MARC_RAW))
      .withInitialRecords(Collections.singletonList(new InitialRecord().withRecord(RAW_RECORD_RESULTING_IN_PARSING_ERROR)));

    Future<Boolean> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> jobExecutionService.setJobProfileToJobExecution(initJobExecutionsRsDto.getParentJobExecutionId(), jobProfileInfo, params))
      .compose(jobExecution -> chunkProcessingService.processChunk(rawRecordsDto, jobExecution.getId(), false, params));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      ArgumentCaptor<StatusDto> captor = ArgumentCaptor.forClass(StatusDto.class);
      Mockito.verify(jobExecutionService, times(1)).updateJobExecutionStatus(anyString(), captor.capture(), isA(OkapiConnectionParams.class));
      context.assertTrue(PARSING_IN_PROGRESS.equals(captor.getAllValues().get(0).getStatus()));
      verify(1, postRequestedFor(urlEqualTo(SNAPSHOT_SERVICE_URL)));
      async.complete();
    });
  }

  @Test
  public void shouldSendEventsWithSuccessfullyParsedRecords(TestContext context) {
    Async async = context.async();

    RawRecordsDto rawRecordsDto = new RawRecordsDto()
      .withRecordsMetadata(new RecordsMetadata()
        .withLast(false)
        .withCounter(1)
        .withContentType(RecordsMetadata.ContentType.MARC_RAW))
      .withInitialRecords(Arrays.asList(
        new InitialRecord().withRecord(CORRECT_RAW_RECORD),
        new InitialRecord().withRecord(CORRECT_RAW_RECORD),
        new InitialRecord().withRecord(RAW_RECORD_RESULTING_IN_PARSING_ERROR)));

    Future<Boolean> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> jobExecutionService.setJobProfileToJobExecution(initJobExecutionsRsDto.getParentJobExecutionId(), jobProfileInfo, params))
      .compose(jobExecution -> chunkProcessingService.processChunk(rawRecordsDto, jobExecution.getId(), false, params));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      ArgumentCaptor<StatusDto> captor = ArgumentCaptor.forClass(StatusDto.class);
      Mockito.verify(jobExecutionService).updateJobExecutionStatus(anyString(), captor.capture(), isA(OkapiConnectionParams.class));
      context.assertTrue(PARSING_IN_PROGRESS.equals(captor.getValue().getStatus()));
      verify(1, postRequestedFor(urlPathEqualTo(SNAPSHOT_SERVICE_URL)));
      async.complete();
    });
  }

  @Test
  public void shouldMarkJobExecutionAsErrorWhenFailedPostRecordsToRecordsStorage(TestContext context) {
    Async async = context.async();
    RawRecordsDto lastRawRecordsDto = new RawRecordsDto()
      .withRecordsMetadata(new RecordsMetadata()
        .withLast(true)
        .withCounter(15)
        .withTotal(15)
        .withContentType(RecordsMetadata.ContentType.MARC_RAW))
      .withInitialRecords(rawRecordsDto.getInitialRecords());

    WireMock.stubFor(WireMock.post(SNAPSHOT_SERVICE_URL)
      .willReturn(WireMock.serverError()));

    Future<Optional<JobExecution>> jobFuture = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> jobExecutionService.setJobProfileToJobExecution(initJobExecutionsRsDto.getParentJobExecutionId(), jobProfileInfo, params))
      .compose(jobExecution -> chunkProcessingService.processChunk(lastRawRecordsDto, jobExecution.getId(), false, params).otherwise(true).map(jobExecution))
      .compose(jobExecution -> jobExecutionService.getJobExecutionById(jobExecution.getId(), params.getTenantId()));

    jobFuture.onComplete(ar -> {
      context.assertTrue(ar.failed());
      verify(1, postRequestedFor(urlEqualTo(SNAPSHOT_SERVICE_URL)));
      async.complete();
    });
  }
}
