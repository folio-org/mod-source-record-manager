package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.folio.dao.JobExecutionDaoImpl;
import org.folio.dao.JobExecutionProgressDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.DataImportInitConfig;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.JobExecutionServiceImpl;
import org.folio.services.progress.JobExecutionProgressServiceImpl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import java.util.Collections;
import java.util.HashMap;

import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INITIALIZATION_STARTED;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.junit.Assert.assertEquals;

@RunWith(VertxUnitRunner.class)
public class DataImportInitConsumerVerticleTest extends AbstractRestTest {
  private static final Integer TOTAL_RECORDS = 10;
  private static final String DEFAULT_NAMESPACE = "folio";

  private Vertx vertx = Vertx.vertx();
  private OkapiConnectionParams params;
  private String jobExecutionId;
  private InitJobExecutionsRqDto initJobExecutionsRqDto = new InitJobExecutionsRqDto()
    .withFiles(Collections.singletonList(new File().withName("importBib1.bib")))
    .withSourceType(InitJobExecutionsRqDto.SourceType.FILES)
    .withUserId(okapiUserIdHeader);

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  @Spy
  private PostgresClientFactory postgresClientFactory = new PostgresClientFactory(vertx);
  @Spy
  @InjectMocks
  private JobExecutionDaoImpl jobExecutionDao;
  @InjectMocks
  @Spy
  private JobExecutionProgressDaoImpl jobExecutionProgressDao;

  @Spy
  @InjectMocks
  private JobExecutionProgressServiceImpl jobExecutionProgressService;
  @Spy
  @InjectMocks
  private JobExecutionServiceImpl jobExecutionService;

  @InjectMocks
  private DataImportInitKafkaHandler initKafkaHandler = new DataImportInitKafkaHandler(vertx, jobExecutionProgressService, jobExecutionService);

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);

    HashMap<String, String> headers = new HashMap<>();
    headers.put(OKAPI_URL_HEADER, "http://localhost:" + snapshotMockServer.port());
    headers.put(OKAPI_TENANT_HEADER, TENANT_ID);
    headers.put(OKAPI_TOKEN_HEADER, "token");
    params = new OkapiConnectionParams(headers, vertx);
  }

  @Test
  public void shouldChangeStatusFromFileUploadedToParsingInProgress(TestContext context) {
    // progress not yet initialized, current status is FILE_UPLOADED,
    // so init handler should initialize progress and change status to PARSING_IN_PROGRESS
    Async async = context.async();
    Future<String> future = prepareJobWithStatus(StatusDto.Status.FILE_UPLOADED);

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());

      assertProgressAndMonitoringAndJobExecutionStatus(JobExecution.Status.PARSING_IN_PROGRESS, async);
    });
  }

  @Test
  public void shouldChangeStatusAndNotBreakAnythingIfProgressAlreadyInitialized(TestContext context) {
    // if progress already initialized, but status not yet changed
    // init handler should use already existed progress and change status to PARSING_IN_PROGRESS
    Async async = context.async();
    Future<String> future = initializeProgressAndPrepareJobWithStatus(StatusDto.Status.FILE_UPLOADED);

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());

      assertProgressAndMonitoringAndJobExecutionStatus(JobExecution.Status.PARSING_IN_PROGRESS, async);
    });
  }

  @Test
  public void shouldNotChangeStatusFromCommittedToParsingInProgress(TestContext context) {
    // if progress already initialized and status already committed(for example when raw mark chunk consumer processed quicker),
    // so init handler should use already existed progress and should remain status the same
    // (it should change status only from FILE_UPLOADED to PARSING_IN_PROGRESS)
    Async async = context.async();
    Future<String> future = initializeProgressAndPrepareJobWithStatus(StatusDto.Status.COMMITTED);

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());

      assertProgressAndMonitoringAndJobExecutionStatus(JobExecution.Status.COMMITTED, async);
    });
  }

  @Test
  public void shouldBeIdempotentWhenStatusAlreadyInProgress(TestContext context) {
    // if progress already initialized and status already in progress(for example when raw mark chunk consumer is processing)
    // so init handler should use already existed progress and should remain status the same
    // (it should change status only from FILE_UPLOADED to PARSING_IN_PROGRESS)
    Async async = context.async();
    Future<String> future = initializeProgressAndPrepareJobWithStatus(StatusDto.Status.PARSING_IN_PROGRESS);

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());

      assertProgressAndMonitoringAndJobExecutionStatus(JobExecution.Status.PARSING_IN_PROGRESS, async);
    });
  }

  private Future<String> prepareJobWithStatus(StatusDto.Status status) {
    return jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(rsDto -> jobExecutionService.updateJobExecutionStatus(rsDto.getParentJobExecutionId(), new StatusDto().withStatus(status), params)
        .compose(jobExecution -> {
          this.jobExecutionId = jobExecution.getId();
          return initKafkaHandler.handle(buildKafkaConsumerRecord(jobExecution.getId()));
        }));
  }

  private Future<String> initializeProgressAndPrepareJobWithStatus(StatusDto.Status status) {
    return jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
        .compose(rsDto -> jobExecutionService.updateJobExecutionStatus(rsDto.getParentJobExecutionId(), new StatusDto().withStatus(status), params)
            .compose(jobExecution -> jobExecutionProgressService.initializeJobExecutionProgress(jobExecution.getId(), TOTAL_RECORDS, TENANT_ID)
              .compose(progress -> {
                this.jobExecutionId = progress.getJobExecutionId();
                return initKafkaHandler.handle(buildKafkaConsumerRecord(progress.getJobExecutionId()));
              })));
  }

  private KafkaConsumerRecord<String, String> buildKafkaConsumerRecord(String jobExecutionId) {
    DataImportInitConfig initConfig = new DataImportInitConfig();
    initConfig.setJobExecutionId(jobExecutionId);
    initConfig.setTotalRecords(TOTAL_RECORDS);

    String topic = KafkaTopicNameHelper.formatTopicName(DEFAULT_NAMESPACE, getDefaultNameSpace(), TENANT_ID, DI_INITIALIZATION_STARTED.value());
    Event event = new Event().withEventPayload(Json.encode(initConfig));
    ConsumerRecord<String, String> consumerRecord = buildConsumerRecord(topic, event);
    return new KafkaConsumerRecordImpl<>(consumerRecord);
  }
}
