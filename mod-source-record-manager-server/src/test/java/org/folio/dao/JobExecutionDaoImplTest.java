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
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.util.Strings;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.File;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDetail;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.JobExecutionDtoCollection;
import org.folio.rest.jaxrs.model.JobExecutionProgress;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.JobMonitoring;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.services.JobExecutionService;
import org.folio.services.JobExecutionServiceImpl;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(VertxUnitRunner.class)
@Log4j2
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
  JobMonitoringDaoImpl jobMonitoringDao;

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
    MockitoAnnotations.initMocks(this);
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

    Instant minusThreeDays = LocalDate.now().minusDays(3).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant();
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

    Instant minusThreeDays = LocalDate.now().minusDays(3).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant();
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

    Instant minusOneDay = LocalDate.now().minusDays(1).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant();
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

    Instant minusThreeDays = LocalDate.now().minusDays(3).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant();
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
    assertThat(createdJobExecutions.size(), Matchers.is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    jobExec.withCompletedDate(Date.from(completedDate));

    JobExecutionProgress jobExecutionProgress = new JobExecutionProgress()
      .withJobExecutionId(jobExec.getId())
      .withTotal(1)
      .withCurrentlySucceeded(1)
      .withCurrentlyFailed(0);

    JobMonitoring jobMonitoring = new JobMonitoring()
      .withId(UUID.randomUUID().toString())
      .withJobExecutionId(jobExec.getId())
      .withNotificationSent(true)
      .withLastEventTimestamp(new Date());

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
      Future<String> saveMonitoringFuture = jobMonitoringDao.save(jobMonitoring, TENANT_ID);
      Future<String> saveJournalFuture = journalRecordDao.save(journalRecord, TENANT_ID);
      Future<String> saveSourceChunkFuture = jobExecutionSourceChunkDao.save(jobExecutionSourceChunk, TENANT_ID);
      return CompositeFuture.all(saveProgressFuture, saveMonitoringFuture, saveJournalFuture, saveSourceChunkFuture)
        .compose(ar -> Future.succeededFuture(jobExecution));
    });
  }

  private void checkDataExistenceAfterHardDeleting(int expectedSize, List<String> jobExecutionIds, Async async) {
    String values = Strings.join(jobExecutionIds, ',');
    fetchInformationFromDatabase(values, JOB_EXECUTION, ID)
      .onSuccess(jobExecutionRows -> {
        assertEquals(expectedSize, Integer.parseInt(jobExecutionRows.value().iterator().next().getValue(0).toString()));

        fetchInformationFromDatabase(values, "job_execution_progress", "job_execution_id")
          .onSuccess(progressRows -> {
            assertEquals(expectedSize, Integer.parseInt(progressRows.value().iterator().next().getValue(0).toString()));

            fetchInformationFromDatabase(values, "job_monitoring", "job_execution_id")
              .onSuccess(monitoringRows -> {
                assertEquals(expectedSize, Integer.parseInt(monitoringRows.value().iterator().next().getValue(0).toString()));

                fetchInformationFromDatabase(values, "journal_records", "job_execution_id")
                  .onSuccess(journalRecordsRows -> {
                    assertEquals(expectedSize, Integer.parseInt(journalRecordsRows.value().iterator().next().getValue(0).toString()));

                    fetchInformationFromDatabase(values, "job_execution_source_chunks", "jobexecutionid")
                      .onSuccess(sourceChunksRows -> {
                        assertEquals(expectedSize, Integer.parseInt(sourceChunksRows.value().iterator().next().getValue(0).toString()));

                        async.complete();
                      });
                  });
              });
          });
      });
  }

  private Future<RowSet<Row>> fetchInformationFromDatabase(String values, String tableName, String fieldName) {
    Promise<RowSet<Row>> selectResult = Promise.promise();
    String preparedQuery = format(GENERIC_SELECT_QUERY_TO_GET_COUNT, convertToPsqlStandard(TENANT_ID) + "." + tableName, fieldName, values);
    postgresClientFactory.createInstance(TENANT_ID).execute(preparedQuery, selectResult);
    return selectResult.future();
  }

}
