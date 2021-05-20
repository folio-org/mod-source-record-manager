package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.dao.JobExecutionDaoImpl;
import org.folio.dao.JobMonitoringDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.JobMonitoring;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;

import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;

@RunWith(VertxUnitRunner.class)
public class JobMonitoringServiceImplTest extends AbstractRestTest {
  private final Vertx vertx = Vertx.vertx();
  @InjectMocks
  private final JobMonitoringService jobMonitoringService = new JobMonitoringServiceImpl();
  private final InitJobExecutionsRqDto initJobExecutionsRqDto = new InitJobExecutionsRqDto()
    .withFiles(Collections.singletonList(new File().withName("importBib1.bib")))
    .withSourceType(InitJobExecutionsRqDto.SourceType.FILES)
    .withUserId(okapiUserIdHeader);
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();
  @Spy
  @InjectMocks
  JobExecutionDaoImpl jobExecutionDao;
  @Spy
  @InjectMocks
  JobExecutionServiceImpl jobExecutionService;
  @Spy
  PostgresClientFactory postgresClientFactory = new PostgresClientFactory(vertx);
  @Spy
  @InjectMocks
  private JobMonitoringDaoImpl jobMonitoringDao;
  private OkapiConnectionParams params;

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
  public void shouldReturnEmptyOptionalWhenRetrieveNonExistingJobMonitoring(TestContext context) {
    Async async = context.async();
    String jobExecutionId = UUID.randomUUID().toString();

    Future<Optional<JobMonitoring>> future = jobMonitoringService.getByJobExecutionId(jobExecutionId, TENANT_ID);

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      Optional<JobMonitoring> optionalJobMonitoring = ar.result();
      context.assertTrue(optionalJobMonitoring.isEmpty());
      async.complete();
    });
  }

  @Test
  public void shouldSaveJobMonitoring(TestContext context) {
    Async async = context.async();
    JobMonitoring givenJobMonitoring = new JobMonitoring();
    givenJobMonitoring.setLastEventTimestamp(new Date());
    givenJobMonitoring.setNotificationSent(true);

    Future<Optional<JobMonitoring>> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> jobMonitoringService.save(givenJobMonitoring.withJobExecutionId(initJobExecutionsRsDto.getParentJobExecutionId()), TENANT_ID))
      .compose(id -> jobMonitoringService.getByJobExecutionId(givenJobMonitoring.getJobExecutionId(), TENANT_ID));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      Optional<JobMonitoring> optionalJobMonitoring = ar.result();
      context.assertTrue(optionalJobMonitoring.isPresent());
      JobMonitoring savedJobMonitoring = optionalJobMonitoring.get();

      context.assertNotNull(savedJobMonitoring.getId());
      context.assertEquals(givenJobMonitoring.getJobExecutionId(), savedJobMonitoring.getJobExecutionId());
      context.assertEquals(givenJobMonitoring.getLastEventTimestamp(), savedJobMonitoring.getLastEventTimestamp());
      context.assertEquals(givenJobMonitoring.getNotificationSent(), savedJobMonitoring.getNotificationSent());
      async.complete();
    });
  }

  @Test
  public void shouldUpdateJobMonitoring(TestContext context) {
    Async async = context.async();
    JobMonitoring givenJobMonitoring = new JobMonitoring();
    givenJobMonitoring.setLastEventTimestamp(new Date());
    givenJobMonitoring.setNotificationSent(true);

    Future<Optional<JobMonitoring>> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> jobMonitoringService.save(givenJobMonitoring.withJobExecutionId(initJobExecutionsRsDto.getParentJobExecutionId()), TENANT_ID))
      .compose(id -> jobMonitoringService.updateByJobExecutionId(givenJobMonitoring.getJobExecutionId(), Timestamp.valueOf(LocalDateTime.now()), false, TENANT_ID))
      .compose(id -> jobMonitoringService.getByJobExecutionId(givenJobMonitoring.getJobExecutionId(), TENANT_ID));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      Optional<JobMonitoring> optionalJobMonitoring = ar.result();
      context.assertTrue(optionalJobMonitoring.isPresent());

      JobMonitoring savedJobMonitoring = optionalJobMonitoring.get();
      context.assertNotNull(savedJobMonitoring.getId());
      context.assertEquals(givenJobMonitoring.getJobExecutionId(), savedJobMonitoring.getJobExecutionId());
      context.assertNotEquals(givenJobMonitoring.getLastEventTimestamp(), savedJobMonitoring.getLastEventTimestamp());
      context.assertNotEquals(givenJobMonitoring.getNotificationSent(), savedJobMonitoring.getNotificationSent());
      async.complete();
    });
  }

  @Test
  public void shouldDeleteJobMonitoring(TestContext context) {
    Async async = context.async();
    JobMonitoring givenJobMonitoring = new JobMonitoring();
    givenJobMonitoring.setLastEventTimestamp(new Date());
    givenJobMonitoring.setNotificationSent(true);

    Future<Optional<JobMonitoring>> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> jobMonitoringService.save(givenJobMonitoring.withJobExecutionId(initJobExecutionsRsDto.getParentJobExecutionId()), TENANT_ID))
      .compose(id -> jobMonitoringService.deleteByJobExecutionId(givenJobMonitoring.getJobExecutionId(), TENANT_ID))
      .compose(id -> jobMonitoringService.getByJobExecutionId(givenJobMonitoring.getJobExecutionId(), TENANT_ID));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      Optional<JobMonitoring> optionalJobMonitoring = ar.result();
      context.assertTrue(optionalJobMonitoring.isEmpty());
      async.complete();
    });
  }
}
