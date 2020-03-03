package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.dao.JobExecutionDaoImpl;
import org.folio.dao.JobExecutionProgressDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.JobExecutionProgress;
import org.folio.services.progress.JobExecutionProgressService;
import org.folio.services.progress.JobExecutionProgressServiceImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;

import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;

@RunWith(VertxUnitRunner.class)
public class JobExecutionProgressServiceImplTest extends AbstractRestTest {

  private Vertx vertx = Vertx.vertx();
  @Spy
  PostgresClientFactory postgresClientFactory = new PostgresClientFactory(vertx);
  @Spy
  @InjectMocks
  JobExecutionDaoImpl jobExecutionDao;
  @Spy
  @InjectMocks
  JobExecutionServiceImpl jobExecutionService;
  @InjectMocks
  @Spy
  private JobExecutionProgressDaoImpl jobExecutionProgressDao;
  @InjectMocks
  private JobExecutionProgressService jobExecutionProgressService = new JobExecutionProgressServiceImpl();

  private OkapiConnectionParams params;

  private InitJobExecutionsRqDto initJobExecutionsRqDto = new InitJobExecutionsRqDto()
    .withFiles(Collections.singletonList(new File().withName("importBib1.bib")))
    .withSourceType(InitJobExecutionsRqDto.SourceType.FILES)
    .withUserId(okapiUserIdHeader);

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
  public void shouldInitProgress(TestContext context) {
    Async async = context.async();
    int expectedTotalRecords = 62;

    Future<JobExecutionProgress> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> jobExecutionProgressService.initializeJobExecutionProgress(initJobExecutionsRsDto.getParentJobExecutionId(), expectedTotalRecords, TENANT_ID))
      .compose(progress -> jobExecutionProgressService.getByJobExecutionId(progress.getJobExecutionId(), TENANT_ID));

    future.setHandler(ar -> {
      context.assertTrue(ar.succeeded());
      JobExecutionProgress progress = ar.result();
      context.assertEquals(expectedTotalRecords, progress.getTotal());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenJobExecutionDoesNotExist(TestContext context) {
    Async async = context.async();
    int totalRecords = 62;
    String jobExecId = UUID.randomUUID().toString();

    Future<JobExecutionProgress> future = jobExecutionProgressService.initializeJobExecutionProgress(jobExecId, totalRecords, TENANT_ID);

    future.setHandler(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldUpdateProgress(TestContext context) {
    Async async = context.async();
    int expectedTotalRecords = 42;
    int expectedSucceededRecords = 40;
    int expectedFailedRecords = 2;

    Future<JobExecutionProgress> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params))
      .compose(initJobExecutionsRsDto -> jobExecutionProgressService.initializeJobExecutionProgress(initJobExecutionsRsDto.getParentJobExecutionId(), expectedTotalRecords, TENANT_ID))
      .compose(progress -> jobExecutionProgressService.updateJobExecutionProgress(progress.getJobExecutionId(), progressToUpdate ->
        progressToUpdate.withCurrentlySucceeded(expectedSucceededRecords).withCurrentlyFailed(expectedFailedRecords),
        TENANT_ID));

    future.setHandler(ar -> {
      context.assertTrue(ar.succeeded());
      JobExecutionProgress progress = ar.result();
      context.assertEquals(expectedTotalRecords, progress.getTotal());
      context.assertEquals(expectedSucceededRecords, progress.getCurrentlySucceeded());
      context.assertEquals(expectedFailedRecords, progress.getCurrentlyFailed());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureOnUpdateWhenProgressDoesNotExist(TestContext context) {
    Async async = context.async();
    int succeededRecords = 7;
    String jobExecutionId = UUID.randomUUID().toString();

    Future<JobExecutionProgress> future = jobExecutionProgressService.updateJobExecutionProgress(jobExecutionId, progressToUpdate ->
      progressToUpdate.withCurrentlySucceeded(succeededRecords), TENANT_ID);

    future.setHandler(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }
}
