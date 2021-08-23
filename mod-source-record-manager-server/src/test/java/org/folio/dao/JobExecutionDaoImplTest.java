package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.*;
import org.folio.services.JobExecutionService;
import org.folio.services.JobExecutionServiceImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.jaxrs.model.JobExecution.SubordinationType.CHILD;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;

@RunWith(VertxUnitRunner.class)
public class JobExecutionDaoImplTest extends AbstractRestTest {
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  private Vertx vertx = Vertx.vertx();
  @Spy
  PostgresClientFactory postgresClientFactory = new PostgresClientFactory(vertx);
  @Spy
  @InjectMocks
  JobExecutionDaoImpl jobExecutionDao;
  @InjectMocks
  JobExecutionService jobExecutionService = new JobExecutionServiceImpl();
  @InjectMocks
  private JobExecutionProgressDao jobExecutionProgressDao = new JobExecutionProgressDaoImpl();

  private OkapiConnectionParams params;

  private InitJobExecutionsRqDto initJobExecutionsRqDto = new InitJobExecutionsRqDto()
    .withFiles(Arrays.asList(
      new File().withName("importBib1.bib"),
      new File().withName("importBib2.bib")))
    .withSourceType(InitJobExecutionsRqDto.SourceType.FILES)
    .withUserId(okapiUserIdHeader);

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    jobExecutionDao.init();

    HashMap<String, String> headers = new HashMap<>();
    headers.put(OKAPI_URL_HEADER, "http://localhost:" + snapshotMockServer.port());
    headers.put(OKAPI_TENANT_HEADER, TENANT_ID);
    headers.put(OKAPI_TOKEN_HEADER, "token");
    params = new OkapiConnectionParams(headers, vertx);
  }

  @Test
  public void shouldSubstituteJobExecutionProgressToJobExecutions(TestContext context) {
    Async async = context.async();

    Future<List<JobExecutionDto>> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .map(InitJobExecutionsRsDto::getJobExecutions)
      .compose(this::createProgressForJobExecutions)
      .compose(ar -> jobExecutionDao.getJobExecutionsWithoutParentMultiple(null, 0, 10, params.getTenantId()))
      .map(JobExecutionCollectionDto::getJobExecutions);

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      Async async2 = context.async(ar.result().size());
      for (JobExecutionDto jobExecution : ar.result()) {
        jobExecutionProgressDao.getByJobExecutionId(jobExecution.getId(), params.getTenantId()).onComplete(progressAr -> {
          context.assertTrue(progressAr.succeeded());
          context.assertTrue(progressAr.result().isPresent());
          JobExecutionProgress progress = progressAr.result().get();
          context.assertEquals(progress.getTotal(), jobExecution.getProgress().getTotal());
          context.assertEquals(progress.getCurrentlySucceeded() + progress.getCurrentlyFailed(), jobExecution.getProgress().getCurrent());
          async2.countDown();
        });
      }
      async.complete();
    });
  }

  private Future<String> createProgressForJobExecutions(List<JobExecution> jobExecutions) {
    Random random = new Random();
    Future<String> future = Future.succeededFuture();

    for (JobExecution jobExecution : jobExecutions) {
      if (jobExecution.getSubordinationType().equals(CHILD)) {
        JobExecutionProgress progress = new JobExecutionProgress().withJobExecutionId(jobExecution.getId())
          .withCurrentlySucceeded(random.nextInt(10))
          .withCurrentlyFailed(random.nextInt(10))
          .withTotal(random.nextInt(10));

        future = future.compose(ar -> jobExecutionProgressDao.save(progress, params.getTenantId()));
      }
    }
    return future;
  }

}
