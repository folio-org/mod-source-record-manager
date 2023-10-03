package org.folio.dao;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.apache.logging.log4j.util.Strings;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCompositeDetailsDto;
import org.folio.rest.jaxrs.model.JobExecutionDetail;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.JobExecutionDtoCollection;
import org.folio.rest.jaxrs.model.JobExecutionProgress;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.JobExecutionDto.SubordinationType;
import org.folio.services.JobExecutionService;
import org.folio.services.JobExecutionServiceImpl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static java.lang.String.format;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.jaxrs.model.JobExecution.SubordinationType.CHILD;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.COMPLETED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.CREATE;
import static org.folio.rest.persist.PostgresClient.convertToPsqlStandard;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(VertxUnitRunner.class)
public class JobExecutionDaoImplTest extends AbstractRestTest {
  public static final String GENERIC_SELECT_QUERY_TO_GET_COUNT = "select count(*) from %s where %s IN ('%s')";
  public static final String JOB_EXECUTION = "job_execution";
  public static final String ID = "id";
  @Rule
  public RunTestOnContext rule = new RunTestOnContext();

  private Vertx vertx = Vertx.vertx();
  @Spy
  PostgresClientFactory postgresClientFactory = new PostgresClientFactory(vertx);
  @Spy
  @InjectMocks
  JobExecutionDaoImpl jobExecutionDao;

  @Spy
  @InjectMocks
  JournalRecordDaoImpl journalRecordDao;

  @Spy
  @InjectMocks
  JobExecutionSourceChunkDaoImpl jobExecutionSourceChunkDao;
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
    MockitoAnnotations.openMocks(this);
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
      .compose(ar -> jobExecutionDao.getJobExecutionsWithoutParentMultiple(new JobExecutionFilter(), null, 0, 10, params.getTenantId()))
      .map(JobExecutionDtoCollection::getJobExecutions);

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

  private Future<RowSet<Row>> createProgressForJobExecutions(List<JobExecution> jobExecutions) {
    Random random = new Random();
    Future<RowSet<Row>> future = Future.succeededFuture();

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

  @Test
  public void shouldDeleteRecordsMarkedForJobExecutionsDeletion(TestContext context) {
    /*
      In this test we setup job execution that finishes 3 days ago, after this job execution has been soft deleted.
      Periodic job should hard delete it, because it checks soft deleted jobs older than 2 days.
    */
    Async async = context.async();

    Instant minusThreeDays = LocalDateTime.now().minus(3, ChronoUnit.DAYS).atOffset(ZoneOffset.UTC).toInstant();
    Future<JobExecution> preparationFuture = prepareDataForDeletion(minusThreeDays);

    preparationFuture.onComplete(ar -> {
      JobExecution jobExecution = ar.result();
      List<String> jobExecutionIds = List.of(jobExecution.getId());

      jobExecutionDao.softDeleteJobExecutionsByIds(jobExecutionIds, TENANT_ID)
        .onSuccess(reply -> {
          JobExecutionDetail jobExecutionDetail = reply.getJobExecutionDetails().get(0);
          assertEquals(jobExecutionDetail.getJobExecutionId(), jobExecution.getId());
          assertTrue(jobExecutionDetail.getIsDeleted());

          jobExecutionDao.hardDeleteJobExecutions(2, TENANT_ID)
            .onSuccess(resp -> checkDataExistenceAfterHardDeleting(0, jobExecutionIds, async));
        });
    });
  }

  @Test
  public void shouldNotDeleteIfJobExecutionIdsNotMatch(TestContext context) {
    /*
      In this test we setup job execution that finishes 3 days ago, after this ANOTHER job execution has been soft deleted.
      Periodic job should not hard delete our initial job execution, because it remains not soft deleted.
    */
    Async async = context.async();

    Instant minusThreeDays = LocalDateTime.now().minus(3, ChronoUnit.DAYS).atOffset(ZoneOffset.UTC).toInstant();
    Future<JobExecution> preparationFuture = prepareDataForDeletion(minusThreeDays);

    preparationFuture.onComplete(ar -> {
      JobExecution jobExecution = ar.result();

      List<String> differentJobExecutionIds = List.of(UUID.randomUUID().toString());
      jobExecutionDao.softDeleteJobExecutionsByIds(differentJobExecutionIds, TENANT_ID)
        .onSuccess(reply -> jobExecutionDao.hardDeleteJobExecutions(2, TENANT_ID)
          .onSuccess(resp -> checkDataExistenceAfterHardDeleting(1, List.of(jobExecution.getId()), async)));
    });
  }

  @Test
  public void shouldNotDeleteIfCompletedDateNotExceeds(TestContext context) {
    /*
      In this test we setup job execution that finishes 1 day ago, after this job execution has been soft deleted.
      Periodic job should not hard delete it, because 1 day does not meet condition: older than 2 days.
    */
    Async async = context.async();

    Instant minusOneDay = LocalDateTime.now().minus(1, ChronoUnit.DAYS).atOffset(ZoneOffset.UTC).toInstant();
    Future<JobExecution> preparationFuture = prepareDataForDeletion(minusOneDay);
    preparationFuture.onComplete(ar -> {
      JobExecution jobExecution = ar.result();
      List<String> jobExecutionIds = List.of(jobExecution.getId());

      jobExecutionDao.softDeleteJobExecutionsByIds(jobExecutionIds, TENANT_ID)
        .onSuccess(reply -> {
          JobExecutionDetail jobExecutionDetail = reply.getJobExecutionDetails().get(0);
          assertEquals(jobExecutionDetail.getJobExecutionId(), jobExecution.getId());
          assertTrue(jobExecutionDetail.getIsDeleted());

          jobExecutionDao.hardDeleteJobExecutions(2, TENANT_ID)
            .onSuccess(resp -> checkDataExistenceAfterHardDeleting(1, jobExecutionIds, async));
        });
    });
  }

  @Test
  public void shouldNotDeleteIfJobHasNotSoftDeleted(TestContext context) {
    /*
      In this test we setup job execution that finishes 3 day ago, but this job has not been soft deleted.
      Periodic job should not hard delete it.
    */
    Async async = context.async();

    Instant minusThreeDays = LocalDateTime.now().minus(3, ChronoUnit.DAYS).atOffset(ZoneOffset.UTC).toInstant();
    Future<JobExecution> preparationFuture = prepareDataForDeletion(minusThreeDays);
    preparationFuture.onComplete(ar -> {
      JobExecution jobExecution = ar.result();
      List<String> jobExecutionIds = List.of(jobExecution.getId());

      jobExecutionDao.hardDeleteJobExecutions(2, TENANT_ID)
        .onSuccess(resp -> checkDataExistenceAfterHardDeleting(1, jobExecutionIds, async));
    });
  }

  private Future<JobExecution> prepareDataForDeletion(Instant completedDate) {
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    jobExec.withCompletedDate(Date.from(completedDate));

    JobExecutionProgress jobExecutionProgress = new JobExecutionProgress()
      .withJobExecutionId(jobExec.getId())
      .withTotal(1)
      .withCurrentlySucceeded(1)
      .withCurrentlyFailed(0);

    JournalRecord journalRecord = new JournalRecord()
      .withJobExecutionId(jobExec.getId())
      .withSourceRecordOrder(0)
      .withSourceId(UUID.randomUUID().toString())
      .withEntityType(JournalRecord.EntityType.MARC_BIBLIOGRAPHIC)
      .withEntityId(UUID.randomUUID().toString())
      .withActionType(CREATE)
      .withActionDate(new Date())
      .withActionStatus(COMPLETED);

    JobExecutionSourceChunk jobExecutionSourceChunk = new JobExecutionSourceChunk()
      .withId("67dfac11-1caf-4470-9ad1-d533f6360bdd")
      .withJobExecutionId(jobExec.getId())
      .withLast(false)
      .withState(JobExecutionSourceChunk.State.COMPLETED)
      .withChunkSize(10)
      .withProcessedAmount(42);

    return jobExecutionDao.updateJobExecution(jobExec, TENANT_ID).compose(jobExecution -> {
      Future<RowSet<Row>> saveProgressFuture = jobExecutionProgressDao.save(jobExecutionProgress, TENANT_ID);
      Future<String> saveJournalFuture = journalRecordDao.save(journalRecord, TENANT_ID);
      Future<String> saveSourceChunkFuture = jobExecutionSourceChunkDao.save(jobExecutionSourceChunk, TENANT_ID);
      return CompositeFuture.all(saveProgressFuture, saveJournalFuture, saveSourceChunkFuture)
        .compose(ar -> Future.succeededFuture(jobExecution));
    });
  }

  private void checkDataExistenceAfterHardDeleting(int expectedSize, List<String> jobExecutionIds, Async async) {
    String values = Strings.join(jobExecutionIds, ',');
    fetchInformationFromDatabase(values, "job_execution", "id")
      .onSuccess(jobExecutionRows -> {
        assertEquals(expectedSize, getRowsSize(jobExecutionRows));

        fetchInformationFromDatabase(values, "job_execution_progress", "job_execution_id")
          .onSuccess(progressRows -> {
            assertEquals(expectedSize, getRowsSize(jobExecutionRows));

                fetchInformationFromDatabase(values, "journal_records", "job_execution_id")
                  .onSuccess(journalRecordsRows -> {
                    assertEquals(expectedSize, getRowsSize(jobExecutionRows));

                    fetchInformationFromDatabase(values, "job_execution_source_chunks", "jobexecutionid")
                      .onSuccess(sourceChunksRows -> {
                        assertEquals(expectedSize, getRowsSize(jobExecutionRows));

                        async.complete();
                      });
                  });
              });
          });
  }

  private int getRowsSize(RowSet<Row> rows) {
    return Integer.parseInt(rows.value().iterator().next().getValue(0).toString());
  }

  private Future<RowSet<Row>> fetchInformationFromDatabase(String values, String tableName, String fieldName) {
    Promise<RowSet<Row>> selectResult = Promise.promise();
    String preparedQuery = format(GENERIC_SELECT_QUERY_TO_GET_COUNT, convertToPsqlStandard(TENANT_ID) + "." + tableName, fieldName, values);
    postgresClientFactory.createInstance(TENANT_ID).execute(preparedQuery, selectResult);
    return selectResult.future();
  }

  @Test
  public void testCompositeDetailsAndProgress(TestContext context) {
    // many children so each status can have a unique # of children
    List<JobExecution> executions = constructAndPostCompositeInitJobExecutionRqDto("test-name", 1+2+3+4+5+6+7+8+9+10+11);

    List<Future> futures = new ArrayList<>();

    for (int i = 1; i < 1 + 1; i++) {
      futures.add(setStatusAndRecordsForCompositeChild(executions.get(i),
        new StatusDto().withStatus(StatusDto.Status.NEW),
        1));
    }
    for (int i = 2; i < 2 + 2; i++) {
      futures.add(setStatusAndRecordsForCompositeChild(executions.get(i),
        new StatusDto().withStatus(StatusDto.Status.FILE_UPLOADED),
        2));
    }
    for (int i = 4; i < 4 + 3; i++) {
      futures.add(setStatusAndRecordsForCompositeChild(executions.get(i),
        new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS),
        4));
    }
    for (int i = 7; i < 7 + 4; i++) {
      futures.add(setStatusAndRecordsForCompositeChild(executions.get(i),
        new StatusDto().withStatus(StatusDto.Status.PARSING_FINISHED),
        8));
    }
    for (int i = 11; i < 11 + 5; i++) {
      futures.add(setStatusAndRecordsForCompositeChild(executions.get(i),
        new StatusDto().withStatus(StatusDto.Status.PROCESSING_IN_PROGRESS),
        16));
    }
    for (int i = 16; i < 16 + 6; i++) {
      futures.add(setStatusAndRecordsForCompositeChild(executions.get(i),
        new StatusDto().withStatus(StatusDto.Status.PROCESSING_FINISHED),
        32));
    }
    for (int i = 22; i < 22 + 7; i++) {
      futures.add(setStatusAndRecordsForCompositeChild(executions.get(i),
        new StatusDto().withStatus(StatusDto.Status.COMMIT_IN_PROGRESS),
        64));
    }
    for (int i = 29; i < 29 + 8; i++) {
      futures.add(setStatusAndRecordsForCompositeChild(executions.get(i),
        new StatusDto().withStatus(StatusDto.Status.COMMITTED),
        128));
    }
    for (int i = 37; i < 37 + 9; i++) {
      futures.add(setStatusAndRecordsForCompositeChild(executions.get(i),
        new StatusDto().withStatus(StatusDto.Status.ERROR).withErrorStatus(StatusDto.ErrorStatus.FILE_PROCESSING_ERROR),
        256));
    }
    for (int i = 46; i < 46 + 10; i++) {
      futures.add(setStatusAndRecordsForCompositeChild(executions.get(i),
        new StatusDto().withStatus(StatusDto.Status.DISCARDED),
        512));
    }
    for (int i = 56; i < 56 + 11; i++) {
      futures.add(setStatusAndRecordsForCompositeChild(executions.get(i),
        new StatusDto().withStatus(StatusDto.Status.CANCELLED),
        1024));
    }

    CompositeFuture.all(futures).onComplete(context.asyncAssertSuccess(v -> {
      jobExecutionDao
        .getJobExecutionsWithoutParentMultiple(
          new JobExecutionFilter().withSubordinationTypeNotAny(Arrays.asList(SubordinationType.COMPOSITE_CHILD)),
          null, 0, 100, params.getTenantId()
        )
        .onComplete(context.asyncAssertSuccess(result -> {
          assertThat(result.getTotalRecords(), is(1));

          JobExecutionDto execution = result.getJobExecutions().get(0);
          JobExecutionCompositeDetailsDto compositeDetails = execution.getCompositeDetails();

          assertThat(compositeDetails.getNewState().getChunksCount(), is(1));
          assertThat(compositeDetails.getNewState().getCurrentlyProcessedCount(), is(2));
          assertThat(compositeDetails.getNewState().getTotalRecordsCount(), is(10));

          assertThat(compositeDetails.getFileUploadedState().getChunksCount(), is(2));
          assertThat(compositeDetails.getFileUploadedState().getCurrentlyProcessedCount(), is(2 * 4));
          assertThat(compositeDetails.getFileUploadedState().getTotalRecordsCount(), is(2 * 20));

          assertThat(compositeDetails.getParsingInProgressState().getChunksCount(), is(3));
          assertThat(compositeDetails.getParsingInProgressState().getCurrentlyProcessedCount(), is(3 * 8));
          assertThat(compositeDetails.getParsingInProgressState().getTotalRecordsCount(), is(3 * 40));

          assertThat(compositeDetails.getParsingFinishedState().getChunksCount(), is(4));
          assertThat(compositeDetails.getParsingFinishedState().getCurrentlyProcessedCount(), is(4 * 16));
          assertThat(compositeDetails.getParsingFinishedState().getTotalRecordsCount(), is(4 * 80));

          assertThat(compositeDetails.getProcessingInProgressState().getChunksCount(), is(5));
          assertThat(compositeDetails.getProcessingInProgressState().getCurrentlyProcessedCount(), is(5 * 32));
          assertThat(compositeDetails.getProcessingInProgressState().getTotalRecordsCount(), is(5 * 160));

          assertThat(compositeDetails.getProcessingFinishedState().getChunksCount(), is(6));
          assertThat(compositeDetails.getProcessingFinishedState().getCurrentlyProcessedCount(), is(6 * 64));
          assertThat(compositeDetails.getProcessingFinishedState().getTotalRecordsCount(), is(6 * 320));

          assertThat(compositeDetails.getCommitInProgressState().getChunksCount(), is(7));
          assertThat(compositeDetails.getCommitInProgressState().getCurrentlyProcessedCount(), is(7 * 128));
          assertThat(compositeDetails.getCommitInProgressState().getTotalRecordsCount(), is(7 * 640));

          assertThat(compositeDetails.getCommittedState().getChunksCount(), is(8));
          assertThat(compositeDetails.getCommittedState().getCurrentlyProcessedCount(), is(8 * 256));
          assertThat(compositeDetails.getCommittedState().getTotalRecordsCount(), is(8 * 1280));

          assertThat(compositeDetails.getErrorState().getChunksCount(), is(9));
          assertThat(compositeDetails.getErrorState().getCurrentlyProcessedCount(), is(9 * 512));
          assertThat(compositeDetails.getErrorState().getTotalRecordsCount(), is(9 * 2560));

          assertThat(compositeDetails.getDiscardedState().getChunksCount(), is(10));
          assertThat(compositeDetails.getDiscardedState().getCurrentlyProcessedCount(), is(10 * 1024));
          assertThat(compositeDetails.getDiscardedState().getTotalRecordsCount(), is(10 * 5120));

          assertThat(compositeDetails.getCancelledState().getChunksCount(), is(11));
          assertThat(compositeDetails.getCancelledState().getCurrentlyProcessedCount(), is(11 * 2048));
          assertThat(compositeDetails.getCancelledState().getTotalRecordsCount(), is(11 * 10240));

          assertThat(
            execution.getProgress().getCurrent(),
            is(1 * 2 + 2 * 4 + 3 * 8 + 4 * 16 + 5 * 32 + 6 * 64 + 7 * 128 + 8 * 256 + 9 * 512 + 10 * 1024 + 11 * 2048)
          );
          assertThat(
            execution.getProgress().getTotal(),
            is(1 * 10 + 2 * 20 + 3 * 40 + 4 * 80 + 5 * 160 + 6 * 320 + 7 * 640 + 8 * 1280 + 9 * 2560 + 10 * 5120 + 11 * 10240)
          );
        }));
    }));
  }

  @Test
  public void testCompositeDetailsAndProgressEmpty(TestContext context) {
    constructAndPostCompositeInitJobExecutionRqDto("test-name", 0);

    jobExecutionDao
      .getJobExecutionsWithoutParentMultiple(
        new JobExecutionFilter().withSubordinationTypeNotAny(Arrays.asList(SubordinationType.COMPOSITE_CHILD)),
        null, 0, 100, params.getTenantId()
      )
      .onComplete(context.asyncAssertSuccess(result -> {
        assertThat(result.getTotalRecords(), is(1));

        JobExecutionDto execution = result.getJobExecutions().get(0);

        assertThat(execution.getCompositeDetails(), is(nullValue()));
        assertThat(execution.getCompositeDetails(), is(nullValue()));
      }));
  }

  private Future<Void> setStatusAndRecordsForCompositeChild(JobExecution execution, StatusDto status, int baseCount) {
    updateJobExecutionStatus(execution, status);
    return jobExecutionProgressDao.save(new JobExecutionProgress().withJobExecutionId(execution.getId())
      .withCurrentlySucceeded(baseCount)
      .withCurrentlyFailed(baseCount)
      .withTotal(baseCount * 10), params.getTenantId()).mapEmpty();
  }
}
