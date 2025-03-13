package org.folio.verticle;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import net.mguenther.kafka.junit.ObserveKeyValues;
import org.folio.dao.JobExecutionProgressDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.JobExecutionDtoCollection;
import org.folio.rest.jaxrs.model.JobExecutionProgress;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.JobExecutionService;
import org.folio.services.progress.BatchableJobExecutionProgress;
import org.folio.services.progress.BatchableJobExecutionProgressCodec;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_JOB_COMPLETED;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.services.progress.JobExecutionProgressUtil.getBatchJobProgressProducer;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@RunWith(VertxUnitRunner.class)
public class JobExecutionProgressVerticleTest extends AbstractRestTest {

  private final int AWAIT_TIME = 3;

  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  private Vertx vertx;

  @Mock
  private JobExecutionProgressDao jobExecutionProgressDao;

  @Mock
  private JobExecutionService jobExecutionService;

  private MessageProducer<BatchableJobExecutionProgress> batchJobProgressProducer;
  private String jobExecutionId;
  private String tenantId;

  @Override
  @Before
  public void setUp(TestContext context) {
    MockitoAnnotations.openMocks(this);
    vertx = rule.vertx();
    vertx.eventBus().registerCodec(new BatchableJobExecutionProgressCodec());
    JobExecutionProgressVerticle jobExecutionProgressVerticle =
      new JobExecutionProgressVerticle(jobExecutionProgressDao, jobExecutionService, kafkaConfig);
    vertx.deployVerticle(jobExecutionProgressVerticle,
      context.asyncAssertSuccess());
    batchJobProgressProducer = getBatchJobProgressProducer(vertx);
    jobExecutionId = UUID.randomUUID().toString();
    tenantId = UUID.randomUUID().toString();
  }

  private OkapiConnectionParams createOkapiConnectionParams(String tenantId) {
    HashMap<String, String> headers = new HashMap<>();
    headers.put(OKAPI_URL_HEADER, "http://localhost:8080");
    headers.put(OKAPI_TENANT_HEADER, tenantId);
    headers.put(OKAPI_TOKEN_HEADER, "token");
    return new OkapiConnectionParams(headers, vertx);
  }

  @Test
  public void testSingleProgressUpdate(TestContext context) {
    Async async = context.async();

    // Arrange
    // create job execution
    JobExecution childJobExecution = new JobExecution()
      .withId(UUID.randomUUID().toString())
      .withHrId(1000)
      .withParentJobId(jobExecutionId)
      .withSubordinationType(JobExecution.SubordinationType.COMPOSITE_CHILD)
      .withStatus(JobExecution.Status.NEW)
      .withUiStatus(JobExecution.UiStatus.INITIALIZATION)
      .withSourcePath("importMarc.mrc")
      .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"))
      .withUserId(UUID.randomUUID().toString());

    JobExecution parentJobExecution = new JobExecution()
      .withId(jobExecutionId)
      .withHrId(1000)
      .withSubordinationType(JobExecution.SubordinationType.COMPOSITE_PARENT)
      .withStatus(JobExecution.Status.NEW)
      .withUiStatus(JobExecution.UiStatus.INITIALIZATION)
      .withSourcePath("importMarc.mrc")
      .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"))
      .withUserId(UUID.randomUUID().toString());
    // create job execution progress
    JobExecutionProgress jobExecutionProgress = new JobExecutionProgress().withJobExecutionId(jobExecutionId)
      .withCurrentlyFailed(1)
      .withCurrentlySucceeded(2)
      .withTotal(3);
    BatchableJobExecutionProgress batchableJobExecutionProgress = new BatchableJobExecutionProgress(
      createOkapiConnectionParams(tenantId),
      jobExecutionProgress);
    // return appropriate objects for mocks
    when(jobExecutionService.getJobExecutionById(eq(childJobExecution.getId()), any()))
      .thenReturn(Future.succeededFuture(Optional.of(childJobExecution)));
    when(jobExecutionService.getJobExecutionById(eq(parentJobExecution.getId()), any()))
      .thenReturn(Future.succeededFuture(Optional.of(parentJobExecution)));
    when(jobExecutionProgressDao.updateCompletionCounts(eq(jobExecutionId), anyInt(), anyInt(), any()))
      .thenReturn(Future.succeededFuture(jobExecutionProgress));
    when(jobExecutionService.updateJobExecutionWithSnapshotStatusAsync(any(), any()))
      .thenReturn(Future.succeededFuture(childJobExecution));
    when(jobExecutionService.getJobExecutionCollectionByParentId(eq(parentJobExecution.getId()), anyInt(), anyInt(), any()))
      .thenReturn(Future.succeededFuture(new JobExecutionDtoCollection()
        .withJobExecutions(Collections.singletonList(
          new JobExecutionDto()
            .withId(childJobExecution.getId())
            .withSubordinationType(JobExecutionDto.SubordinationType.COMPOSITE_CHILD)
            .withUiStatus(JobExecutionDto.UiStatus.RUNNING_COMPLETE))
        )
      ));
    var topic = formatToKafkaTopicName(DI_JOB_COMPLETED.value(), tenantId);

    // Act
    batchJobProgressProducer.write(batchableJobExecutionProgress)
      .onComplete(ar -> {
        if (ar.succeeded()) {
          // Assert
          try {
            await()
              .atMost(AWAIT_TIME, TimeUnit.SECONDS)
              .untilAsserted(() -> verify(jobExecutionProgressDao)
                .updateCompletionCounts(any(), eq(2), eq(1), eq(tenantId)));
            kafkaCluster.observeValues(ObserveKeyValues.on(topic, 1)
              .observeFor(30, TimeUnit.SECONDS)
              .build());
          } catch (Exception e) {
            context.fail(e);
          }
          async.complete();
        } else {
          context.fail(ar.cause());
        }
      });
  }

  @Test
  public void testSingleProgressUpdateSplitFileDisabled(TestContext context) {
    Async async = context.async();

    // Arrange
    // create job execution with CHILD subordination Type (simulating non split-files env)
    JobExecution childJobExecution = new JobExecution()
      .withId(UUID.randomUUID().toString())
      .withHrId(1000)
      .withParentJobId(jobExecutionId)
      .withSubordinationType(JobExecution.SubordinationType.CHILD)
      .withStatus(JobExecution.Status.NEW)
      .withUiStatus(JobExecution.UiStatus.INITIALIZATION)
      .withSourcePath("importMarc.mrc")
      .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"))
      .withUserId(UUID.randomUUID().toString());

    JobExecution parentJobExecution = new JobExecution()
      .withId(jobExecutionId)
      .withHrId(1000)
      .withSubordinationType(JobExecution.SubordinationType.PARENT_SINGLE)
      .withStatus(JobExecution.Status.NEW)
      .withUiStatus(JobExecution.UiStatus.INITIALIZATION)
      .withSourcePath("importMarc.mrc")
      .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"))
      .withUserId(UUID.randomUUID().toString());
    // create job execution progress
    JobExecutionProgress jobExecutionProgress = new JobExecutionProgress().withJobExecutionId(jobExecutionId)
      .withCurrentlyFailed(1)
      .withCurrentlySucceeded(2)
      .withTotal(3);
    BatchableJobExecutionProgress batchableJobExecutionProgress = new BatchableJobExecutionProgress(
      createOkapiConnectionParams(tenantId),
      jobExecutionProgress);
    // return appropriate objects for mocks
    when(jobExecutionService.getJobExecutionById(eq(childJobExecution.getId()), any()))
      .thenReturn(Future.succeededFuture(Optional.of(childJobExecution)));
    when(jobExecutionService.getJobExecutionById(eq(parentJobExecution.getId()), any()))
      .thenReturn(Future.succeededFuture(Optional.of(parentJobExecution)));
    when(jobExecutionProgressDao.updateCompletionCounts(eq(jobExecutionId), anyInt(), anyInt(), any()))
      .thenReturn(Future.succeededFuture(jobExecutionProgress));
    when(jobExecutionService.updateJobExecutionWithSnapshotStatus(any(), any()))
      .thenReturn(Future.succeededFuture(childJobExecution));
    when(jobExecutionService.getJobExecutionCollectionByParentId(eq(parentJobExecution.getId()), anyInt(), anyInt(), any()))
      .thenReturn(Future.succeededFuture(new JobExecutionDtoCollection()
        .withJobExecutions(Collections.singletonList(
          new JobExecutionDto()
            .withId(childJobExecution.getId())
            .withSubordinationType(JobExecutionDto.SubordinationType.COMPOSITE_CHILD)
            .withUiStatus(JobExecutionDto.UiStatus.RUNNING_COMPLETE)))
      ));

    var topic = formatToKafkaTopicName(DI_JOB_COMPLETED.value(), tenantId);

    // Act
    batchJobProgressProducer.write(batchableJobExecutionProgress)
      .onComplete(ar -> {
        if (ar.succeeded()) {
          // Assert
          try {
            await()
              .atMost(AWAIT_TIME, TimeUnit.SECONDS)
              .untilAsserted(() -> verify(jobExecutionProgressDao)
                .updateCompletionCounts(any(), eq(2), eq(1), eq(tenantId)));
            kafkaCluster.observeValues(ObserveKeyValues.on(topic, 1)
              .observeFor(30, TimeUnit.SECONDS)
              .build());
          } catch (Exception e) {
            context.fail(e);
          }
          async.complete();
        } else {
          context.fail(ar.cause());
        }
      });
  }

  @Test
  public void testMultipleProgressUpdateShouldBatch(TestContext context) {
    Async async = context.async();

    // Arrange
    // create job execution
    JobExecution jobExecution = new JobExecution()
      .withId(jobExecutionId)
      .withHrId(1000)
      .withParentJobId(jobExecutionId)
      .withSubordinationType(JobExecution.SubordinationType.PARENT_SINGLE)
      .withStatus(JobExecution.Status.NEW)
      .withUiStatus(JobExecution.UiStatus.INITIALIZATION)
      .withSourcePath("importMarc.mrc")
      .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"))
      .withUserId(UUID.randomUUID().toString());
    // create job execution progress
    JobExecutionProgress jobExecutionProgress = new JobExecutionProgress().withJobExecutionId(jobExecutionId)
      .withCurrentlyFailed(0)
      .withCurrentlySucceeded(1)
      .withTotal(3);
    BatchableJobExecutionProgress batchableJobExecutionProgress = new BatchableJobExecutionProgress(
      createOkapiConnectionParams(tenantId),
      jobExecutionProgress);
    // return appropriate objects for mocks
    when(jobExecutionService.getJobExecutionById(any(), any()))
      .thenReturn(Future.succeededFuture(Optional.of(jobExecution)));
    when(jobExecutionProgressDao.updateCompletionCounts(eq(jobExecutionId), anyInt(), anyInt(), any()))
      .thenReturn(Future.succeededFuture(jobExecutionProgress));

    // Act
    batchJobProgressProducer.write(batchableJobExecutionProgress);
    batchJobProgressProducer.write(batchableJobExecutionProgress);
    batchJobProgressProducer.write(batchableJobExecutionProgress)
      .onComplete(ar -> {
        if (ar.succeeded()) {
          // Assert
          try {
            await()
              .atMost(AWAIT_TIME, TimeUnit.SECONDS)
              .untilAsserted(() -> verify(jobExecutionProgressDao)
                .updateCompletionCounts(jobExecutionId, 3, 0, tenantId));
          } catch (Exception e) {
            context.fail(e);
          }
          async.complete();
        } else {
          context.fail(ar.cause());
        }
      });
  }

  @Test
  public void testErrorDuringProgressUpdate(TestContext context) {
    Async async = context.async();

    // Arrange
    // create job execution
    JobExecution childJobExecution = new JobExecution()
      .withId(UUID.randomUUID().toString())
      .withHrId(1000)
      .withParentJobId(jobExecutionId)
      .withSubordinationType(JobExecution.SubordinationType.COMPOSITE_CHILD)
      .withStatus(JobExecution.Status.NEW)
      .withUiStatus(JobExecution.UiStatus.INITIALIZATION)
      .withSourcePath("importMarc.mrc")
      .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"))
      .withUserId(UUID.randomUUID().toString());
    // create job execution progress
    JobExecutionProgress jobExecutionProgress = new JobExecutionProgress().withJobExecutionId(jobExecutionId)
      .withCurrentlyFailed(1)
      .withCurrentlySucceeded(2)
      .withTotal(3);
    BatchableJobExecutionProgress batchableJobExecutionProgress = new BatchableJobExecutionProgress(
      createOkapiConnectionParams(tenantId),
      jobExecutionProgress);
    // return appropriate objects for mocks
    when(jobExecutionService.getJobExecutionById(eq(childJobExecution.getId()), any()))
      .thenReturn(Future.succeededFuture(Optional.of(childJobExecution)));
    when(jobExecutionProgressDao.updateCompletionCounts(eq(jobExecutionId), anyInt(), anyInt(), any()))
      .thenReturn(Future.failedFuture(new RuntimeException("Something Happened")))
      .thenReturn(Future.succeededFuture(jobExecutionProgress));


    // Act
    batchJobProgressProducer.write(batchableJobExecutionProgress)
      .compose(ar -> {
          // Assert that job execution was updated to error state
          await()
            .atMost(AWAIT_TIME, TimeUnit.SECONDS)
            .untilAsserted(() -> {
              verify(jobExecutionProgressDao, times(1))
                .updateCompletionCounts(any(), anyInt(), anyInt(), eq(tenantId));

              ArgumentCaptor<StatusDto> argumentCaptor = ArgumentCaptor.forClass(StatusDto.class);
              verify(jobExecutionService).updateJobExecutionStatus(any(), argumentCaptor.capture(), any());
              StatusDto statusDto = argumentCaptor.getValue();
              context.assertEquals(StatusDto.Status.ERROR, statusDto.getStatus());
            });
          return Future.succeededFuture();
      })
      // Ensure that the consumer is able to process messages after encountering an error
      .compose(notUsed ->
        batchJobProgressProducer.write(batchableJobExecutionProgress)
      )
      .compose(notUsed -> {
        await()
          .atMost(AWAIT_TIME, TimeUnit.SECONDS)
          .untilAsserted(() -> verify(jobExecutionProgressDao, times(2))
            .updateCompletionCounts(any(), anyInt(), anyInt(), eq(tenantId)));
        return Future.succeededFuture();
      })
      .onSuccess(notUsed -> async.complete())
      .onFailure(th -> context.fail(th.getCause()));
  }

  @Test
  public void testCommittedDuringExtraProgressUpdate(TestContext context) {
    Async async = context.async();

    // Arrange
    // create job execution
    JobExecution jobExecution = new JobExecution()
      .withId(jobExecutionId)
      .withHrId(1000)
      .withParentJobId(jobExecutionId)
      .withSubordinationType(JobExecution.SubordinationType.PARENT_SINGLE)
      .withStatus(JobExecution.Status.NEW)
      .withUiStatus(JobExecution.UiStatus.INITIALIZATION)
      .withSourcePath("importMarc.mrc")
      .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"))
      .withUserId(UUID.randomUUID().toString());
    // create job execution progress
    JobExecutionProgress jobExecutionProgress = new JobExecutionProgress().withJobExecutionId(jobExecutionId)
      .withCurrentlyFailed(0)
      .withCurrentlySucceeded(2)
      .withTotal(1);
    BatchableJobExecutionProgress batchableJobExecutionProgress = new BatchableJobExecutionProgress(
      createOkapiConnectionParams(tenantId),
      jobExecutionProgress);
    // return appropriate objects for mocks
    when(jobExecutionService.getJobExecutionById(any(), any()))
      .thenReturn(Future.succeededFuture(Optional.of(jobExecution)));
    when(jobExecutionProgressDao.updateCompletionCounts(eq(jobExecutionId), anyInt(), anyInt(), any()))
      .thenReturn(Future.succeededFuture(jobExecutionProgress));
    when(jobExecutionService.updateJobExecutionWithSnapshotStatus(any(), any()))
      .thenReturn(Future.succeededFuture(jobExecution));


    // Act
    batchJobProgressProducer.write(batchableJobExecutionProgress)
      .onComplete(ar -> {
        if (ar.succeeded()) {
          // Assert
          try {
            await()
              .atMost(AWAIT_TIME, TimeUnit.SECONDS)
              .untilAsserted(() -> {
                verify(jobExecutionProgressDao)
                  .updateCompletionCounts(jobExecutionId, 2, 0, tenantId);

                ArgumentCaptor<JobExecution> argumentCaptor = ArgumentCaptor.forClass(JobExecution.class);
                verify(jobExecutionService).updateJobExecutionWithSnapshotStatus(argumentCaptor.capture(), any());
                JobExecution.Status status = argumentCaptor.getValue().getStatus();
                context.assertEquals(JobExecution.Status.COMMITTED, status);
              });
          } catch (Exception e) {
            context.fail(e);
          }
          async.complete();
        } else {
          context.fail(ar.cause());
        }
      });
  }

  @Test
  public void testJobExecutionIsNotUpdatedIfJobExecutionAlreadyCompleted(TestContext context) {
    Async async = context.async();
    // Arrange
    // simulate job execution that has already been completed with COMMITTED status
    JobExecution jobExecution = new JobExecution()
      .withId(UUID.randomUUID().toString())
      .withHrId(1000)
      .withParentJobId(jobExecutionId)
      .withSubordinationType(JobExecution.SubordinationType.COMPOSITE_CHILD)
      .withStatus(JobExecution.Status.COMMITTED)
      .withUiStatus(JobExecution.UiStatus.RUNNING_COMPLETE)
      .withCompletedDate(new Date())
      .withSourcePath("importMarc.mrc")
      .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"))
      .withUserId(UUID.randomUUID().toString());
    context.assertEquals(JobExecution.Status.COMMITTED, jobExecution.getStatus());
    context.assertEquals(JobExecution.UiStatus.RUNNING_COMPLETE, jobExecution.getUiStatus());

    // simulate job execution progress calculated after job completion upon receiving DI_ERROR event,
    // produced as a result of re-consuming previously received event by another module
    JobExecutionProgress jobExecutionProgress = new JobExecutionProgress().withJobExecutionId(jobExecution.getId())
      .withCurrentlyFailed(1)
      .withCurrentlySucceeded(3)
      .withTotal(3);
    BatchableJobExecutionProgress batchableJobExecutionProgress =
      new BatchableJobExecutionProgress(createOkapiConnectionParams(tenantId), jobExecutionProgress);

    when(jobExecutionProgressDao.updateCompletionCounts(eq(jobExecution.getId()), anyInt(), anyInt(), any()))
      .thenReturn(Future.succeededFuture(jobExecutionProgress));
    when(jobExecutionService.getJobExecutionById(jobExecution.getId(), tenantId))
      .thenReturn(Future.succeededFuture(Optional.of(jobExecution)));

    // Act
    Future<Void> future = batchJobProgressProducer.write(batchableJobExecutionProgress);

    // Assert
    future.onComplete(ar -> context.verify(v -> {
      context.assertTrue(ar.succeeded());
      await()
        .atMost(AWAIT_TIME, TimeUnit.SECONDS)
        .untilAsserted(() ->
          verify(jobExecutionProgressDao).updateCompletionCounts(jobExecution.getId(), 3, 1, tenantId));
      verify(jobExecutionService, never())
        .updateJobExecutionWithSnapshotStatus(argThat(job -> job.getId().equals(jobExecution.getId())), any());
      async.complete();
    }));
  }

}
