package org.folio.services;

import static com.github.tomakehurst.wiremock.client.WireMock.created;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.jaxrs.model.JobExecution.Status.COMMITTED;
import static org.folio.rest.jaxrs.model.JobExecution.Status.ERROR;
import static org.folio.rest.jaxrs.model.JobExecution.Status.PARSING_IN_PROGRESS;
import static org.folio.rest.jaxrs.model.JobExecution.UiStatus.RUNNING_COMPLETE;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import org.folio.DataImportEventPayload;
import org.folio.TestUtil;
import org.folio.dao.JobExecutionDaoImpl;
import org.folio.dao.JobExecutionProgressDaoImpl;
import org.folio.dao.JobExecutionSourceChunkDaoImpl;
import org.folio.dao.JournalRecordDaoImpl;
import org.folio.dao.MappingParamsSnapshotDaoImpl;
import org.folio.dao.MappingRuleDaoImpl;
import org.folio.dao.MappingRulesSnapshotDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.marc.MarcRecordAnalyzer;
import org.folio.kafka.KafkaConfig;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionProgress;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.JobProfileInfo.DataType;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.RecordsMetadata;
import org.folio.services.afterprocessing.FieldModificationServiceImpl;
import org.folio.services.afterprocessing.HrIdFieldServiceImpl;
import org.folio.services.journal.JournalServiceImpl;
import org.folio.services.mappers.processor.MappingParametersProvider;
import org.folio.services.progress.JobExecutionProgressServiceImpl;
import org.folio.services.validation.JobProfileSnapshotValidationServiceImpl;
import org.folio.verticle.consumers.util.MarcImportEventsHandler;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.test.util.ReflectionTestUtils;

@RunWith(VertxUnitRunner.class)
public class RecordProcessedEventHandlingServiceImplTest extends AbstractRestTest {

  private static final String CORRECT_RAW_RECORD = "01240cas a2200397   450000100070000000500170000700800410002401000170006502200140008203500260009603500220012203500110014403500190015504000440017405000150021808200110023322200420024424500430028626000470032926500380037630000150041431000220042932100250045136200230047657000290049965000330052865000450056165500420060670000450064885300180069386300230071190200160073490500210075094800370077195000340080836683220141106221425.0750907c19509999enkqr p       0   a0eng d  a   58020553   a0022-0469  a(CStRLIN)NYCX1604275S  a(NIC)notisABP6388  a366832  a(OCoLC)1604275  dCtYdMBTIdCtYdMBTIdNICdCStRLINdNIC0 aBR140b.J6  a270.0504aThe Journal of ecclesiastical history04aThe Journal of ecclesiastical history.  aLondon,bCambridge University Press [etc.]  a32 East 57th St., New York, 10022  av.b25 cm.  aQuarterly,b1970-  aSemiannual,b1950-690 av. 1-   Apr. 1950-  aEditor:   C. W. Dugmore. 0aChurch historyxPeriodicals. 7aChurch history2fast0(OCoLC)fst00860740 7aPeriodicals2fast0(OCoLC)fst014116411 aDugmore, C. W.q(Clifford William),eed.0381av.i(year)4081a1-49i1950-1998  apfndbLintz  a19890510120000.02 a20141106bmdbatcheltsxaddfast  lOLINaBR140b.J86h01/01/01 N01542ccm a2200361   ";
  private static final String RULES_PATH = "src/test/resources/org/folio/services/marc_bib_rules.json";
  private static final String KAFKA_ENV_ID = "test-env";
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();
  @Spy
  private Vertx vertx = Vertx.vertx();
  @Spy
  private PostgresClientFactory postgresClientFactory = new PostgresClientFactory(vertx);
  @Spy
  @InjectMocks
  private JobExecutionDaoImpl jobExecutionDao;
  @Spy
  @InjectMocks
  private JobExecutionSourceChunkDaoImpl jobExecutionSourceChunkDao;
  @Spy
  @InjectMocks
  private MappingRuleServiceImpl mappingRuleService;
  @Spy
  @InjectMocks
  private MappingRuleDaoImpl mappingRuleDao;
  @Spy
  @InjectMocks
  private MappingParametersProvider mappingParametersProvider;
  @InjectMocks
  @Spy
  private JobExecutionServiceImpl jobExecutionService;
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
  private JournalServiceImpl journalService;
  @Spy
  @InjectMocks
  private MarcImportEventsHandler marcImportEventsHandler;
  @Spy
  @InjectMocks
  private MappingRulesSnapshotDaoImpl mappingRulesSnapshotDao;
  @Spy
  @InjectMocks
  private MappingParamsSnapshotDaoImpl mappingParamsSnapshotDao;
  @Spy
  @InjectMocks
  private FieldModificationServiceImpl fieldModificationService;

  @Spy
  RecordsPublishingService recordsPublishingService;
  private MappingRuleCache mappingRuleCache;
  private ChangeEngineService changeEngineService;
  private ChunkProcessingService chunkProcessingService;
  private RecordProcessedEventHandlingServiceImpl recordProcessedEventHandlingService;
  private OkapiConnectionParams params;
  private MappingMetadataService mappingMetadataService;
  private KafkaConfig kafkaConfig;

  private InitJobExecutionsRqDto initJobExecutionsRqDto = new InitJobExecutionsRqDto()
    .withFiles(Collections.singletonList(new File().withName("importBib1.bib")))
    .withSourceType(InitJobExecutionsRqDto.SourceType.FILES)
    .withUserId(okapiUserIdHeader);

  private RawRecordsDto rawRecordsDto = new RawRecordsDto()
    .withId(UUID.randomUUID().toString())
    .withRecordsMetadata(new RecordsMetadata()
      .withLast(false)
      .withCounter(1)
      .withTotal(15)
      .withContentType(RecordsMetadata.ContentType.MARC_RAW))
    .withInitialRecords(Collections.singletonList(new InitialRecord().withRecord(CORRECT_RAW_RECORD)));

  private JobProfileInfo jobProfileInfo = new JobProfileInfo()
    .withName("MARC records")
    .withId(jobProfile.getId())
    .withDataType(DataType.MARC);

  private final JsonObject userResponse = new JsonObject()
    .put("users",
      new JsonArray().add(new JsonObject()
        .put("username", "diku_admin")
        .put("personal", new JsonObject().put("firstName", "DIKU").put("lastName", "ADMINISTRATOR"))))
    .put("totalRecords", 1);

  @Before
  public void setUp() throws IOException {
    String[] hostAndPort = kafkaCluster.getBrokerList().split(":");
    kafkaConfig = KafkaConfig.builder()
      .kafkaHost(hostAndPort[0])
      .kafkaPort(hostAndPort[1])
      .envId(KAFKA_ENV_ID)
      .build();
    String rules = TestUtil.readFileFromPath(RULES_PATH);

    MockitoAnnotations.openMocks(this);

    mappingRuleCache = new MappingRuleCache(mappingRuleDao, vertx);
    marcRecordAnalyzer = new MarcRecordAnalyzer();
    mappingRuleService = new MappingRuleServiceImpl(mappingRuleDao, mappingRuleCache);
    mappingRuleDao = when(mock(MappingRuleDaoImpl.class).get(any(), anyString())).thenReturn(Future.succeededFuture(Optional.of(new JsonObject(rules)))).getMock();
    mappingParametersProvider = when(mock(MappingParametersProvider.class).get(anyString(), any(OkapiConnectionParams.class))).thenReturn(Future.succeededFuture(new MappingParameters())).getMock();
    mappingMetadataService = new MappingMetadataServiceImpl(mappingParametersProvider, mappingRuleService, mappingRulesSnapshotDao, mappingParamsSnapshotDao);
    JobProfileSnapshotValidationServiceImpl jobProfileSnapshotValidationService = new JobProfileSnapshotValidationServiceImpl();
    changeEngineService = new ChangeEngineServiceImpl(jobExecutionSourceChunkDao, jobExecutionService, marcRecordAnalyzer, hrIdFieldService , recordsPublishingService, mappingMetadataService, jobProfileSnapshotValidationService, kafkaConfig,
      fieldModificationService);
    ReflectionTestUtils.setField(changeEngineService, "maxDistributionNum", 10);
    ReflectionTestUtils.setField(changeEngineService, "batchSize", 100);
    chunkProcessingService = new EventDrivenChunkProcessingServiceImpl(jobExecutionSourceChunkDao, jobExecutionService, changeEngineService, jobExecutionProgressService);
    recordProcessedEventHandlingService = new RecordProcessedEventHandlingServiceImpl(jobExecutionProgressService, jobExecutionService);
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

  @Test
  public void shouldIncrementCurrentlySucceededAndUpdateProgressOnHandleEvent(TestContext context) {
    // given
    Async async = context.async();
    HashMap<String, String> payloadContext = new HashMap<>();
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DataImportEventTypes.DI_COMPLETED.value())
      .withContext(payloadContext);

    Future<Boolean> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> jobExecutionService.setJobProfileToJobExecution(initJobExecutionsRsDto.getParentJobExecutionId(), jobProfileInfo, params))
      .compose(jobExecution -> {
        dataImportEventPayload.setJobExecutionId(jobExecution.getId());
        return chunkProcessingService.processChunk(rawRecordsDto, jobExecution.getId(), params);
      });

    // when
    Future<JobExecutionProgress> jobFuture = future
      .compose(ar -> recordProcessedEventHandlingService.handle(Json.encode(dataImportEventPayload), params))
      .compose(ar -> jobExecutionProgressService.getByJobExecutionId(dataImportEventPayload.getJobExecutionId(), TENANT_ID));

    // then
    jobFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      JobExecutionProgress updatedProgress = ar.result();
      context.assertEquals(1, updatedProgress.getCurrentlySucceeded());
      context.assertEquals(0, updatedProgress.getCurrentlyFailed());
      context.assertEquals(rawRecordsDto.getRecordsMetadata().getTotal(), updatedProgress.getTotal());
      async.complete();
    });
  }

  @Test
  public void shouldIncrementCurrentlyFailedAndUpdateProgressOnHandleEvent(TestContext context) {
    // given
    Async async = context.async();
    HashMap<String, String> payloadContext = new HashMap<>();
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DataImportEventTypes.DI_ERROR.value())
      .withContext(payloadContext);

    Future<Boolean> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> jobExecutionService.setJobProfileToJobExecution(initJobExecutionsRsDto.getParentJobExecutionId(), jobProfileInfo, params))
      .compose(jobExecution -> {
        dataImportEventPayload.setJobExecutionId(jobExecution.getId());
        return chunkProcessingService.processChunk(rawRecordsDto, jobExecution.getId(), params);
      });

    // when
    Future<JobExecutionProgress> jobFuture = future
      .compose(ar -> recordProcessedEventHandlingService.handle(Json.encode(dataImportEventPayload), params))
      .compose(ar -> jobExecutionProgressService.getByJobExecutionId(dataImportEventPayload.getJobExecutionId(), TENANT_ID));

    // then
    jobFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      JobExecutionProgress updatedProgress = ar.result();
      context.assertEquals(1, updatedProgress.getCurrentlyFailed());
      context.assertEquals(0, updatedProgress.getCurrentlySucceeded());
      context.assertEquals(rawRecordsDto.getRecordsMetadata().getTotal(), updatedProgress.getTotal());

      Async async2 = context.async();
      jobFuture.compose(jobAr -> jobExecutionService.getJobExecutionById(dataImportEventPayload.getJobExecutionId(), TENANT_ID))
        .onComplete(jobAr -> {
          context.assertTrue(jobAr.succeeded());
          context.assertTrue(jobAr.result().isPresent());
          JobExecution jobExecution = jobAr.result().get();
          context.assertEquals(PARSING_IN_PROGRESS, jobExecution.getStatus());
          async2.complete();
        });
      async.complete();
    });
  }

  @Test
  public void shouldMarkJobExecutionAsCommittedOnHandleEventWhenAllRecordsSuccessfullyProcessed(TestContext context) {
    // given
    Async async = context.async();
    RawRecordsDto rawRecordsDto = new RawRecordsDto()
      .withInitialRecords(Collections.singletonList(new InitialRecord().withRecord(CORRECT_RAW_RECORD)))
      .withRecordsMetadata(new RecordsMetadata()
        .withLast(false)
        .withCounter(1)
        .withTotal(1)
        .withContentType(RecordsMetadata.ContentType.MARC_RAW));

    HashMap<String, String> payloadContext = new HashMap<>();
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DataImportEventTypes.DI_COMPLETED.value())
      .withContext(payloadContext);

    Future<Boolean> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> jobExecutionService.setJobProfileToJobExecution(initJobExecutionsRsDto.getParentJobExecutionId(), jobProfileInfo, params))
      .compose(jobExecution -> {
        dataImportEventPayload.setJobExecutionId(jobExecution.getId());
        return chunkProcessingService.processChunk(rawRecordsDto, jobExecution.getId(), params);
      });

    // when
    Future<Optional<JobExecution>> jobFuture = future
      .compose(ar -> recordProcessedEventHandlingService.handle(Json.encode(dataImportEventPayload), params))
      .compose(ar -> jobExecutionService.getJobExecutionById(dataImportEventPayload.getJobExecutionId(), TENANT_ID));

    // then
    jobFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertTrue(ar.result().isPresent());
      JobExecution jobExecution = ar.result().get();
      context.assertEquals(COMMITTED, jobExecution.getStatus());
      context.assertEquals(RUNNING_COMPLETE, jobExecution.getUiStatus());
      context.assertEquals(rawRecordsDto.getRecordsMetadata().getTotal(), jobExecution.getProgress().getTotal());
      context.assertNotNull(jobExecution.getStartedDate());
      context.assertNotNull(jobExecution.getCompletedDate());
      async.complete();
    });
  }

  @Test
  public void shouldMarkJobExecutionAsErrorOnHandleDIErrorEventWhenAllRecordsProcessed(TestContext context) {
    // given
    Async async = context.async();
    RawRecordsDto rawRecordsDto = new RawRecordsDto()
      .withInitialRecords(Collections.singletonList(new InitialRecord().withRecord(CORRECT_RAW_RECORD)))
      .withRecordsMetadata(new RecordsMetadata()
        .withLast(true)
        .withCounter(2)
        .withTotal(2)
        .withContentType(RecordsMetadata.ContentType.MARC_RAW));

    HashMap<String, String> payloadContext = new HashMap<>();
    DataImportEventPayload datImpErrorEventPayload = new DataImportEventPayload()
      .withEventType(DataImportEventTypes.DI_ERROR.value())
      .withContext(payloadContext);

    DataImportEventPayload datImpOtherEventPayload = new DataImportEventPayload()
      .withEventType(DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_UPDATED.value())
      .withContext(payloadContext);

    DataImportEventPayload datImpCompletedEventPayload = new DataImportEventPayload()
      .withEventType(DataImportEventTypes.DI_COMPLETED.value())
      .withContext(payloadContext);

    Future<Boolean> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> jobExecutionService.setJobProfileToJobExecution(initJobExecutionsRsDto.getParentJobExecutionId(), jobProfileInfo, params))
      .map(jobExecution -> {
        datImpErrorEventPayload.withJobExecutionId(jobExecution.getId());
        return datImpCompletedEventPayload.withJobExecutionId(jobExecution.getId());
      })
      .compose(ar -> chunkProcessingService.processChunk(rawRecordsDto, datImpErrorEventPayload.getJobExecutionId(), params));

    // when
    Future<Optional<JobExecution>> jobFuture = future
      .compose(ar -> recordProcessedEventHandlingService.handle(Json.encode(datImpErrorEventPayload), params))
      .compose(ar -> recordProcessedEventHandlingService.handle(Json.encode(datImpOtherEventPayload), params))
      .compose(ar -> recordProcessedEventHandlingService.handle(Json.encode(datImpCompletedEventPayload), params))
      .compose(ar -> jobExecutionService.getJobExecutionById(datImpCompletedEventPayload.getJobExecutionId(), TENANT_ID));

    // then
    jobFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertTrue(ar.result().isPresent());
      JobExecution jobExecution = ar.result().get();
      context.assertEquals(ERROR, jobExecution.getStatus());
      context.assertEquals(JobExecution.UiStatus.ERROR, jobExecution.getUiStatus());
      context.assertEquals(rawRecordsDto.getRecordsMetadata().getTotal(), jobExecution.getProgress().getTotal());
      context.assertNotNull(jobExecution.getStartedDate());
      context.assertNotNull(jobExecution.getCompletedDate());
      verify(2, putRequestedFor(new UrlPathPattern(new RegexPattern(SNAPSHOT_SERVICE_URL + "/.*"), true)));
      async.complete();
    });
  }

  @Test
  public void shouldMarkJobExecutionAsErrorOnHandleDIOtherEvent(TestContext context) {
    // given
    Async async = context.async();

    HashMap<String, String> payloadContext = new HashMap<>();

    DataImportEventPayload datImpOtherEventPayload = new DataImportEventPayload()
      .withEventType(DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_UPDATED.value())
      .withContext(payloadContext);


    Future<DataImportEventPayload> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> jobExecutionService.setJobProfileToJobExecution(initJobExecutionsRsDto.getParentJobExecutionId(), jobProfileInfo, params))
      .map(jobExecution -> datImpOtherEventPayload.withJobExecutionId(jobExecution.getId()));

    // when
    Future<Boolean> jobFuture = future
      .compose(ar -> recordProcessedEventHandlingService.handle(Json.encode(datImpOtherEventPayload), params));

    // then
    jobFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertFalse(ar.result());
      async.complete();
    });
  }
}
