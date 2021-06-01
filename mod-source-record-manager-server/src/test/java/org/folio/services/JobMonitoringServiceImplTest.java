package org.folio.services;

import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
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

    Future<Optional<JobMonitoring>> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> jobMonitoringService.saveNew(initJobExecutionsRsDto.getParentJobExecutionId(), TENANT_ID))
      .compose(jobMonitoring -> jobMonitoringService.getByJobExecutionId(jobMonitoring.getJobExecutionId(), TENANT_ID));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      Optional<JobMonitoring> optionalJobMonitoring = ar.result();
      context.assertTrue(optionalJobMonitoring.isPresent());
      JobMonitoring savedJobMonitoring = optionalJobMonitoring.get();

      context.assertNotNull(savedJobMonitoring.getId());
      context.assertNotNull(savedJobMonitoring.getJobExecutionId());
      context.assertNotNull(savedJobMonitoring.getLastEventTimestamp());
      context.assertFalse(savedJobMonitoring.getNotificationSent());
      async.complete();
    });
  }

  @Test
  public void shouldUpdateJobMonitoring(TestContext context) {
    Async async = context.async();

    Future<Optional<JobMonitoring>> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> jobMonitoringService.saveNew(initJobExecutionsRsDto.getParentJobExecutionId(), TENANT_ID)
        .compose(jobMonitoring -> jobMonitoringService.updateByJobExecutionId(jobMonitoring.getJobExecutionId(), Timestamp.valueOf(LocalDateTime.now()), true, TENANT_ID))
        .compose(updated -> jobMonitoringService.getByJobExecutionId(initJobExecutionsRsDto.getParentJobExecutionId(), TENANT_ID))
      );

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      Optional<JobMonitoring> optionalJobMonitoring = ar.result();
      context.assertTrue(optionalJobMonitoring.isPresent());

      JobMonitoring savedJobMonitoring = optionalJobMonitoring.get();
      context.assertNotNull(savedJobMonitoring.getId());
      context.assertNotNull(savedJobMonitoring.getJobExecutionId());
      context.assertNotNull(savedJobMonitoring.getLastEventTimestamp());
      context.assertTrue(savedJobMonitoring.getNotificationSent());
      async.complete();
    });
  }

  @Test
  public void shouldDeleteJobMonitoring(TestContext context) {
    Async async = context.async();

    Future<Optional<JobMonitoring>> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> jobMonitoringService.saveNew(initJobExecutionsRsDto.getParentJobExecutionId(), TENANT_ID)
        .compose(jobMonitoring -> jobMonitoringService.deleteByJobExecutionId(jobMonitoring.getJobExecutionId(), TENANT_ID))
        .compose(deleted -> jobMonitoringService.getByJobExecutionId(initJobExecutionsRsDto.getParentJobExecutionId(), TENANT_ID))
      );

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      Optional<JobMonitoring> optionalJobMonitoring = ar.result();
      context.assertTrue(optionalJobMonitoring.isEmpty());
      async.complete();
    });
  }

  @Test
  public void shouldFindAllJobMonitoring(TestContext context) {
    Async async = context.async();
    JobMonitoring givenJobMonitoring = new JobMonitoring();
    givenJobMonitoring.setLastEventTimestamp(new Date());
    givenJobMonitoring.setNotificationSent(true);

    Future<List<JobMonitoring>> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> jobMonitoringService.saveNew(initJobExecutionsRsDto.getParentJobExecutionId(), TENANT_ID))
      .compose(initJobExecutionsRsDto2 ->jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params))
      .compose(initJobExecutionsRsDto2 -> jobMonitoringService.saveNew(initJobExecutionsRsDto2.getParentJobExecutionId(), TENANT_ID))
      .compose(list -> jobMonitoringService.getAll(TENANT_ID));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      List<JobMonitoring> jobMonitors = ar.result();
      context.assertTrue(!jobMonitors.isEmpty());
      context.assertTrue(jobMonitors.size() == 2);
      async.complete();
    });
  }
}
