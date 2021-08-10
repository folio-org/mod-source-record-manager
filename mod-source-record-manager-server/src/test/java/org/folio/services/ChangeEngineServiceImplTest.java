//package org.folio.services;
//
//import static org.hamcrest.MatcherAssert.assertThat;
//import static org.hamcrest.Matchers.equalTo;
//import static org.hamcrest.Matchers.hasSize;
//import static org.hamcrest.Matchers.nullValue;
//import static org.junit.Assert.assertTrue;
//import static org.mockito.ArgumentMatchers.any;
//import static org.mockito.ArgumentMatchers.anyList;
//import static org.mockito.Mockito.when;
//
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Optional;
//import java.util.UUID;
//
//import io.vertx.core.Future;
//import io.vertx.core.Vertx;
//import io.vertx.core.json.Json;
//import io.vertx.core.json.JsonObject;
//import org.junit.Before;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.mockito.InjectMocks;
//import org.mockito.Mock;
//import org.mockito.Mockito;
//import org.mockito.junit.MockitoJUnitRunner;
//import org.springframework.test.util.ReflectionTestUtils;
//
//import org.folio.dao.JobExecutionSourceChunkDao;
//import org.folio.dataimport.util.OkapiConnectionParams;
//import org.folio.dataimport.util.marc.MarcRecordAnalyzer;
//import org.folio.dataimport.util.marc.MarcRecordType;
//import org.folio.kafka.KafkaConfig;
//import org.folio.rest.jaxrs.model.ActionProfile;
//import org.folio.rest.jaxrs.model.InitialRecord;
//import org.folio.rest.jaxrs.model.JobExecution;
//import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
//import org.folio.rest.jaxrs.model.JobProfileInfo;
//import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
//import org.folio.rest.jaxrs.model.RawRecordsDto;
//import org.folio.rest.jaxrs.model.Record;
//import org.folio.rest.jaxrs.model.RecordsMetadata;
//import org.folio.services.afterprocessing.HrIdFieldService;
//import org.folio.services.util.EventHandlingUtil;
//
//@RunWith(MockitoJUnitRunner.class)
//public class ChangeEngineServiceImplTest {
//
//  private static final String MARC_HOLDINGS_REC_VALID =
//    "00182cx  a22000851  4500001000900000004000800009005001700017008003300034852002900067\u001E10245123\u001E9928371\u001E20170607135730.0\u001E1706072u    8   4001uu   0901128\u001E0 \u001Fbfine\u001FhN7433.3\u001Fi.B87 2014\u001E\u001D";
//  private static final String MARC_HOLDINGS_REC_WITHOUT_004 =
//    "00162cx  a22000731  4500001000900000005001700009008003300026852002900059\u001E10245123\u001E20170607135730.0\u001E1706072u    8   4001uu   0901128\u001E0 \u001Fbfine\u001FhN7433.3\u001Fi.B87 2014\u001E\u001D";
//  @Mock
//  private JobExecutionSourceChunkDao jobExecutionSourceChunkDao;
//  @Mock
//  private JobExecutionService jobExecutionService;
//  @Mock
//  private MarcRecordAnalyzer marcRecordAnalyzer;
//  @Mock
//  private HrIdFieldService hrIdFieldService;
//  @Mock
//  private RecordsPublishingService recordsPublishingService;
//  @Mock
//  private KafkaConfig kafkaConfig;
//  private OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(new HashMap<>(), Vertx.vertx());
//
//  @InjectMocks
//  private ChangeEngineServiceImpl service;
//
//  @Before
//  public void setUp() throws Exception {
//    ReflectionTestUtils.setField(service, "maxDistributionNum", 10);
//  }
//
//  @Test
//  public void shouldReturnMarcHoldingsRecord() {
//    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(MARC_HOLDINGS_REC_VALID);
//    JobExecution jobExecution = getTestJobExecution();
//
//    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.HOLDING);
//    when(jobExecutionSourceChunkDao.getById(any(), any()))
//      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
//    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));
//
//    Future<List<Record>> serviceFuture = executeWithKafkaMock(rawRecordsDto, jobExecution, Future.succeededFuture(true));
//
//    var actual = serviceFuture.result();
//    assertThat(actual, hasSize(1));
//    assertThat(actual.get(0).getRecordType(), equalTo(Record.RecordType.MARC_HOLDING));
//    assertThat(actual.get(0).getErrorRecord(), nullValue());
//  }
//
//  @Test
//  public void shouldReturnMarcHoldingsRecordWhenProfileHasUpdateAction() {
//    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(MARC_HOLDINGS_REC_VALID);
//    JobExecution jobExecution = getTestJobExecution();
//    jobExecution.setJobProfileSnapshotWrapper(new ProfileSnapshotWrapper()
//      .withChildSnapshotWrappers(List.of(new ProfileSnapshotWrapper()
//        .withContentType(ProfileSnapshotWrapper.ContentType.ACTION_PROFILE)
//        .withContent(new JsonObject(Json.encode(new ActionProfile()
//          .withAction(ActionProfile.Action.UPDATE)
//          .withFolioRecord(ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC))).getMap())
//      ))
//    );
//
//    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.HOLDING);
//    when(jobExecutionSourceChunkDao.getById(any(), any()))
//      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
//    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));
//    when(recordsPublishingService.sendEventsWithRecords(any(), any(), any(), any()))
//      .thenReturn(Future.succeededFuture(true));
//
//    Future<List<Record>> serviceFuture = executeWithKafkaMock(rawRecordsDto, jobExecution, Future.succeededFuture(true));
//
//    var actual = serviceFuture.result();
//    assertThat(actual, hasSize(1));
//    assertThat(actual.get(0).getRecordType(), equalTo(Record.RecordType.MARC_HOLDING));
//    assertThat(actual.get(0).getErrorRecord(), nullValue());
//  }
//
//  @Test
//  public void shouldReturnMarcHoldingsRecordWithErrorsWhen004FieldIsMissing() {
//    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(MARC_HOLDINGS_REC_WITHOUT_004);
//    JobExecution jobExecution = getTestJobExecution();
//
//    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.HOLDING);
//    when(jobExecutionSourceChunkDao.getById(any(), any()))
//      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
//    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));
//
//    Future<List<Record>> serviceFuture = executeWithKafkaMock(rawRecordsDto, jobExecution, Future.succeededFuture(true));
//
//    var actual = serviceFuture.result();
//    assertThat(actual, hasSize(1));
////    assertThat(actual.get(0).getRecordType(), equalTo(Record.RecordType.MARC_HOLDING));
////    assertThat(actual.get(0).getErrorRecord(), notNullValue());
////    assertThat(actual.get(0).getErrorRecord().getDescription(),
////      containsString("The 004 tag of the Holdings doesn't has a link"));
//  }
//
//  @Test
//  public void shouldFailedWhenKafkaFailedToSendEvent() {
//    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(MARC_HOLDINGS_REC_VALID);
//    JobExecution jobExecution = getTestJobExecution();
//
//    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.HOLDING);
//    when(jobExecutionSourceChunkDao.getById(any(), any()))
//      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
//    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));
//    when(jobExecutionService.updateJobExecutionStatus(any(), any(), any())).thenReturn(Future.succeededFuture(jobExecution));
//
//    Future<List<Record>> serviceFuture = executeWithKafkaMock(rawRecordsDto, jobExecution, Future.failedFuture("Failed"));
//
//    assertTrue(serviceFuture.failed());
//  }
//
//  @Test
//  public void shouldFailedWhenKafkaFailedToSendEventAndFailedToUpdateJobExecutionStatus() {
//    RawRecordsDto rawRecordsDto = getTestRawRecordsDto(MARC_HOLDINGS_REC_VALID);
//    JobExecution jobExecution = getTestJobExecution();
//
//    when(marcRecordAnalyzer.process(any())).thenReturn(MarcRecordType.HOLDING);
//    when(jobExecutionSourceChunkDao.getById(any(), any()))
//      .thenReturn(Future.succeededFuture(Optional.of(new JobExecutionSourceChunk())));
//    when(jobExecutionSourceChunkDao.update(any(), any())).thenReturn(Future.succeededFuture(new JobExecutionSourceChunk()));
//    when(jobExecutionService.updateJobExecutionStatus(any(), any(), any())).thenReturn(Future.failedFuture("Failed"));
//
//    Future<List<Record>> serviceFuture = executeWithKafkaMock(rawRecordsDto, jobExecution, Future.failedFuture("Failed"));
//
//    assertTrue(serviceFuture.failed());
//  }
//
//  private RawRecordsDto getTestRawRecordsDto(String marcHoldingsRecValid) {
//    return new RawRecordsDto().withId(UUID.randomUUID().toString())
//      .withRecordsMetadata(new RecordsMetadata().withContentType(RecordsMetadata.ContentType.MARC_RAW))
//      .withInitialRecords(Collections.singletonList(new InitialRecord().withRecord(marcHoldingsRecValid)));
//  }
//
//  private JobExecution getTestJobExecution() {
//    return new JobExecution().withId(UUID.randomUUID().toString())
//      .withJobProfileSnapshotWrapper(new ProfileSnapshotWrapper())
//      .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString())
//        .withName("test").withDataType(JobProfileInfo.DataType.MARC));
//  }
//
//  private Future<List<Record>> executeWithKafkaMock(RawRecordsDto rawRecordsDto, JobExecution jobExecution,
//                                                    Future<Boolean> eventSentResult) {
//    try (var mockedStatic = Mockito.mockStatic(EventHandlingUtil.class)) {
//      mockedStatic.when(() -> EventHandlingUtil.sendEventToKafka(any(), any(), any(), anyList(), any(), any()))
//        .thenReturn(eventSentResult);
//      return service.parseRawRecordsChunkForJobExecution(rawRecordsDto, jobExecution, "1", okapiConnectionParams);
//    }
//  }
//}
