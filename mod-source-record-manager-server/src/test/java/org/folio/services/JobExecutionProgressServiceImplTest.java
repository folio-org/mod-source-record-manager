package org.folio.services;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.dao.JobExecutionDaoImpl;
import org.folio.dao.JobExecutionProgressDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionProgress;
import org.folio.services.progress.JobExecutionProgressService;
import org.folio.services.progress.JobExecutionProgressServiceImpl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import javax.ws.rs.BadRequestException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class JobExecutionProgressServiceImplTest extends AbstractRestTest {
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

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
  private JobExecutionProgressService jobExecutionProgressService = new JobExecutionProgressServiceImpl(vertx);

  private OkapiConnectionParams params;

  private final InitJobExecutionsRqDto initJobExecutionsRqDto = new InitJobExecutionsRqDto()
    .withFiles(Collections.singletonList(new File().withName("importBib1.bib")))
    .withSourceType(InitJobExecutionsRqDto.SourceType.FILES)
    .withUserId(okapiUserIdHeader);

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);

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

    future.onComplete(ar -> {
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

    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldNotReturnFailedFutureWhenJobExecutionIdDuplicates(TestContext context) {
    Async async = context.async();
    int expectedTotalRecords = 62;

    Future<CompositeFuture> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> {
        Future<JobExecutionProgress> future1 = jobExecutionProgressService.initializeJobExecutionProgress(initJobExecutionsRsDto.getParentJobExecutionId(), expectedTotalRecords, TENANT_ID);
        Future<JobExecutionProgress> future2 = jobExecutionProgressService.initializeJobExecutionProgress(initJobExecutionsRsDto.getParentJobExecutionId(), expectedTotalRecords, TENANT_ID);
        return GenericCompositeFuture.join(Arrays.asList(future1, future2));
      });

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
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

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      JobExecutionProgress progress = ar.result();
      context.assertEquals(expectedTotalRecords, progress.getTotal());
      context.assertEquals(expectedSucceededRecords, progress.getCurrentlySucceeded());
      context.assertEquals(expectedFailedRecords, progress.getCurrentlyFailed());
      async.complete();
    });
  }

  @Test
  public void shouldUpdateCounts(TestContext context) {
    Async async = context.async();
    int expectedTotalRecords = 42;
    int expectedSucceededRecords = 40;
    int expectedFailedRecords = 2;

    Future<Void> future = jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params)
      .compose(initJobExecutionsRsDto -> jobExecutionService.initializeJobExecutions(initJobExecutionsRqDto, params))
      .compose(initJobExecutionsRsDto -> jobExecutionProgressService.initializeJobExecutionProgress(initJobExecutionsRsDto.getParentJobExecutionId(), expectedTotalRecords, TENANT_ID))
      .compose(progress ->
        jobExecutionProgressService.updateCompletionCounts(progress.getJobExecutionId(), expectedSucceededRecords,
          expectedFailedRecords, params));

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
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

    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void updateJobExecutionWithSnapshotStatusAsyncHandlesParentJobWithCompletedStatus(TestContext context) {
    Async async = context.async();

    JobExecution parentJobExecution = new JobExecution()
      .withId(UUID.randomUUID().toString())
      .withSubordinationType(JobExecution.SubordinationType.COMPOSITE_PARENT)
      .withUiStatus(JobExecution.UiStatus.RUNNING_COMPLETE);

    when(jobExecutionDao.getJobExecutionById(eq(parentJobExecution.getId()), anyString()))
      .thenReturn(Future.succeededFuture(Optional.of(parentJobExecution)));

    Future<JobExecution> future = jobExecutionService.updateJobExecutionWithSnapshotStatusAsync(parentJobExecution, params);
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      context.assertEquals(
        String.format("updateJobExecutionWithSnapshotStatusAsync:: Parent job with jobExecutionId=%s already has completed status. Skipping update.", parentJobExecution.getId()),
        ar.cause().getMessage()
      );
      verify(jobExecutionDao, never()).updateBlocking(anyString(), any(), anyString());
      async.complete();
    });
  }

  @Test
  public void updateJobExecutionWithSnapshotStatusAsyncHandlesParentJobWithoutCompletedStatus(TestContext context) {
    Async async = context.async();

    JobExecution parentJobExecution = new JobExecution()
      .withId(UUID.randomUUID().toString())
      .withSubordinationType(JobExecution.SubordinationType.COMPOSITE_PARENT)
      .withStatus(JobExecution.Status.PROCESSING_IN_PROGRESS)
      .withUiStatus(JobExecution.UiStatus.RUNNING);

    when(jobExecutionDao.getJobExecutionById(eq(parentJobExecution.getId()), anyString()))
      .thenReturn(Future.succeededFuture(Optional.of(parentJobExecution)));
    when(jobExecutionDao.updateBlocking(eq(parentJobExecution.getId()), any(), anyString()))
      .thenReturn(Future.succeededFuture(parentJobExecution));

    Future<JobExecution> future = jobExecutionService.updateJobExecutionWithSnapshotStatusAsync(parentJobExecution, params);

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(parentJobExecution, ar.result());
      verify(jobExecutionDao).updateBlocking(anyString(), any(), anyString());
      async.complete();
    });
  }

  @Test
  public void updateJobExecutionWithSnapshotStatusAsyncFailsWhenParentJobNotFound(TestContext context) {
    Async async = context.async();

    JobExecution parentJobExecution = new JobExecution()
      .withId(UUID.randomUUID().toString())
      .withSubordinationType(JobExecution.SubordinationType.COMPOSITE_PARENT)
      .withStatus(JobExecution.Status.PROCESSING_IN_PROGRESS)
      .withUiStatus(JobExecution.UiStatus.RUNNING);

    when(jobExecutionDao.getJobExecutionById(eq(parentJobExecution.getId()), anyString()))
      .thenReturn(Future.succeededFuture(Optional.empty()));

    Future<JobExecution> future = jobExecutionService.updateJobExecutionWithSnapshotStatusAsync(parentJobExecution, params);

    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      context.assertEquals(
        String.format("updateJobExecutionWithSnapshotStatusAsync:: Couldn't find parent job with jobExecutionId=%s", parentJobExecution.getId()),
        ar.cause().getMessage()
      );
      async.complete();
    });
  }

  @Test
  public void updateJobExecutionWithSnapshotStatusAsyncUpdatesNonParentJob(TestContext context) {
    Async async = context.async();

    JobExecution jobExecution = new JobExecution()
      .withId(UUID.randomUUID().toString())
      .withStatus(JobExecution.Status.COMMITTED)
      .withSubordinationType(JobExecution.SubordinationType.CHILD);

    when(jobExecutionDao.updateBlocking(eq(jobExecution.getId()), any(), anyString()))
      .thenReturn(Future.succeededFuture(jobExecution));

    Future<JobExecution> future = jobExecutionService.updateJobExecutionWithSnapshotStatusAsync(jobExecution, params);

    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(jobExecution, ar.result());
      verify(jobExecutionDao).updateBlocking(anyString(), any(), anyString());
      async.complete();
    });
  }

  @Test
  public void shouldSendOnlyOneEventOnRaceCondition(TestContext context) {
    Async async = context.async();

    String parentJobId = UUID.randomUUID().toString();

    JobExecution parentToUpdate = new JobExecution()
      .withId(parentJobId)
      .withSubordinationType(JobExecution.SubordinationType.COMPOSITE_PARENT)
      .withStatus(JobExecution.Status.COMMITTED)
      .withUiStatus(JobExecution.UiStatus.RUNNING_COMPLETE);

    JobExecution initialStateInDb = new JobExecution()
      .withId(parentJobId)
      .withSubordinationType(JobExecution.SubordinationType.COMPOSITE_PARENT)
      .withStatus(JobExecution.Status.PROCESSING_IN_PROGRESS)
      .withUiStatus(JobExecution.UiStatus.RUNNING);

    when(jobExecutionDao.getJobExecutionById(eq(parentJobId), anyString()))
      .thenReturn(Future.succeededFuture(Optional.of(initialStateInDb)))
      .thenReturn(Future.succeededFuture(Optional.of(initialStateInDb)))
      .thenReturn(Future.succeededFuture(Optional.of(parentToUpdate)));

    AtomicBoolean firstUpdateSucceeded = new AtomicBoolean(false);
    doAnswer(invocation -> {
      if (firstUpdateSucceeded.compareAndSet(false, true)) {
        return Future.succeededFuture(parentToUpdate);
      } else {
        return Future.failedFuture(new BadRequestException("DAO: Job is already committed"));
      }
    }).when(jobExecutionDao).updateBlocking(eq(parentJobId), any(), anyString());

    doReturn(Future.succeededFuture(new JobExecution()))
      .when(jobExecutionService).updateSnapshotStatus(any(JobExecution.class), any(OkapiConnectionParams.class));

    Future<JobExecution> winnerAttempt = jobExecutionService.updateJobExecutionWithSnapshotStatusAsync(parentToUpdate, params);
    Future<JobExecution> loserAttemptDAO = jobExecutionService.updateJobExecutionWithSnapshotStatusAsync(parentToUpdate, params);
    Future<JobExecution> loserAttemptService = jobExecutionService.updateJobExecutionWithSnapshotStatusAsync(parentToUpdate, params);

    Future.all(winnerAttempt, loserAttemptDAO, loserAttemptService).onComplete(ar -> {
      context.assertTrue(winnerAttempt.succeeded(), "The winner must finish successfully.");
      context.assertNotNull(winnerAttempt.result());

      context.assertTrue(loserAttemptDAO.failed(), "The loser (via DAO) should finish with an error.");
      context.assertTrue(loserAttemptDAO.cause() instanceof BadRequestException);
      context.assertTrue(loserAttemptDAO.cause().getMessage().contains("DAO: Job is already committed"));

      context.assertTrue(loserAttemptService.failed(), "The loser (via the service) finish end with an error.");
      context.assertTrue(loserAttemptService.cause() instanceof BadRequestException);
      context.assertTrue(loserAttemptService.cause().getMessage().contains("already has completed status. Skipping update."));

      async.complete();
    });
  }
}
