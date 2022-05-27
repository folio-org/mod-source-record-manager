package org.folio.dao;

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
  public void shouldDeleteRecordsMarkedForJobExecutionsDeletion(TestContext context) throws InterruptedException {
    Async async = context.async();
    InitJobExecutionsRsDto response =
      constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), Matchers.is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    jobExec.withCompletedDate(Date.from(LocalDate.now().minusDays(3).atStartOfDay().atZone(ZoneId.systemDefault()).toInstant()));
    JobExecutionProgress jobExecutionProgress = new JobExecutionProgress().withJobExecutionId(jobExec.getId()).withTotal(1).withCurrentlySucceeded(1).withCurrentlyFailed(0);
    JobMonitoring jobMonitoring = new JobMonitoring().withId(UUID.randomUUID().toString()).withJobExecutionId(jobExec.getId()).withNotificationSent(true).withLastEventTimestamp(new Date());
    JournalRecord journalRecord1 = new JournalRecord()
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
    Future<JobExecution> stringFuture = jobExecutionDao.updateJobExecution(jobExec, TENANT_ID).compose(jobExecution1 -> {
      jobExecutionProgressDao.save(jobExecutionProgress, TENANT_ID).onSuccess(rows -> log.info("jobExecutionProgress updated successfully"));
      jobMonitoringDao.save(jobMonitoring, TENANT_ID).onSuccess(rows -> log.info("jobMonitoring updated successfully"));
      journalRecordDao.save(journalRecord1, TENANT_ID).onSuccess(rows -> log.info("journalRecord updated successfully"));
      jobExecutionSourceChunkDao.save(jobExecutionSourceChunk, TENANT_ID).onSuccess(rows -> log.info("jobExec Source Chunk updated successfully"));
      return Future.succeededFuture(jobExecution1);
    });

    Thread.sleep(5000);
    stringFuture.onComplete(ar -> {
      assertTrue(ar.succeeded());

      List<String> jobExecutionIds = Arrays.asList(ar.result().getId());
      String values = Strings.join(jobExecutionIds, ',');

      fetchInformationFromDatabase(values, GENERIC_SELECT_QUERY_TO_GET_COUNT, JOB_EXECUTION, ID)
        .onSuccess(rows1 -> {
          log.debug(JOB_EXECUTION + "Before Deletion " + Integer.parseInt(rows1.value().iterator().next().getValue(0).toString()));
          assertEquals(1, Integer.parseInt(rows1.value().iterator().next().getValue(0).toString()));
        });

      fetchInformationFromDatabase(values, GENERIC_SELECT_QUERY_TO_GET_COUNT, "job_execution_progress", "job_execution_id")
        .onSuccess(rows1 -> {
          log.debug("job_execution_source_chunks Before Deletion " + Integer.parseInt(rows1.value().iterator().next().getValue(0).toString()));
          assertEquals(1, Integer.parseInt(rows1.value().iterator().next().getValue(0).toString()));
        });

      fetchInformationFromDatabase(values, GENERIC_SELECT_QUERY_TO_GET_COUNT, "job_monitoring", "job_execution_id")
        .onSuccess(rows1 -> {
          log.debug("job_monitoring Before Deletion " + Integer.parseInt(rows1.value().iterator().next().getValue(0).toString()));
          assertEquals(1, Integer.parseInt(rows1.value().iterator().next().getValue(0).toString()));
        });

      fetchInformationFromDatabase(values, GENERIC_SELECT_QUERY_TO_GET_COUNT, "journal_records", "job_execution_id")
        .onSuccess(rows1 -> {
          log.debug("journal_records Before Deletion " + Integer.parseInt(rows1.value().iterator().next().getValue(0).toString()));
          assertEquals(1, Integer.parseInt(rows1.value().iterator().next().getValue(0).toString()));
        });

      fetchInformationFromDatabase(values, GENERIC_SELECT_QUERY_TO_GET_COUNT, "job_execution_source_chunks", "jobexecutionid")
        .onSuccess(rows1 -> {
          log.debug("job_execution_source_chunks Before Deletion " + Integer.parseInt(rows1.value().iterator().next().getValue(0).toString()));
          assertEquals(1, Integer.parseInt(rows1.value().iterator().next().getValue(0).toString()));
        });
      jobExecutionDao.softDeleteJobExecutionsByIds(jobExecutionIds, TENANT_ID)
        .onSuccess(deleteJobExecutionsResp -> {
          assertEquals(deleteJobExecutionsResp.getJobExecutionDetails().get(0).getJobExecutionId(), jobExecutionIds.get(0));
          assertTrue(deleteJobExecutionsResp.getJobExecutionDetails().get(0).getIsDeleted());

          jobExecutionDao.hardDeleteJobExecutions(TENANT_ID, 2)
            .onSuccess(rows -> {
              jobExecutionDao.getJobExecutionById(deleteJobExecutionsResp.getJobExecutionDetails().get(0).getJobExecutionId(), TENANT_ID)
                .onSuccess(optionalJobExecution -> {
                  assertTrue(optionalJobExecution.isEmpty());
                  fetchInformationFromDatabase(values, GENERIC_SELECT_QUERY_TO_GET_COUNT, JOB_EXECUTION, ID)
                    .onSuccess(rows1 -> {
                      log.debug(JOB_EXECUTION + "After Deletion " + Integer.parseInt(rows1.value().iterator().next().getValue(0).toString()));
                      assertEquals(0, Integer.parseInt(rows1.value().iterator().next().getValue(0).toString()));
                    });
                  fetchInformationFromDatabase(values, GENERIC_SELECT_QUERY_TO_GET_COUNT, "job_execution_progress", "job_execution_id")
                    .onSuccess(rows1 -> {
                      log.debug("job_execution_source_chunks After Deletion " + Integer.parseInt(rows1.value().iterator().next().getValue(0).toString()));
                      assertEquals(0, Integer.parseInt(rows1.value().iterator().next().getValue(0).toString()));
                    });
                  fetchInformationFromDatabase(values, GENERIC_SELECT_QUERY_TO_GET_COUNT, "job_monitoring", "job_execution_id")
                    .onSuccess(rows1 -> {
                      log.debug("job_monitoring After Deletion " + Integer.parseInt(rows1.value().iterator().next().getValue(0).toString()));
                      assertEquals(0, Integer.parseInt(rows1.value().iterator().next().getValue(0).toString()));
                    });
                  fetchInformationFromDatabase(values, GENERIC_SELECT_QUERY_TO_GET_COUNT, "journal_records", "job_execution_id")
                    .onSuccess(rows1 -> {
                      log.debug("journal_records After Deletion " + Integer.parseInt(rows1.value().iterator().next().getValue(0).toString()));
                      assertEquals(0, Integer.parseInt(rows1.value().iterator().next().getValue(0).toString()));
                    });
                  fetchInformationFromDatabase(values, GENERIC_SELECT_QUERY_TO_GET_COUNT, "job_execution_source_chunks", "jobexecutionid")
                    .onSuccess(rows1 -> {
                      log.debug("job_execution_source_chunks Before Deletion " + Integer.parseInt(rows1.value().iterator().next().getValue(0).toString()));
                      assertEquals(0, Integer.parseInt(rows1.value().iterator().next().getValue(0).toString()));
                      async.complete();
                    });
                });
            });
        });
    });
  }

  private Future<RowSet<Row>> fetchInformationFromDatabase(String values, String query, String tableName, String fieldName) {
    Promise<RowSet<Row>> selectResult = Promise.promise();
    String preparedQuery = format(query, convertToPsqlStandard(TENANT_ID) + "." + tableName, fieldName, values);
    postgresClientFactory.createInstance(TENANT_ID).execute(preparedQuery, selectResult);
    return selectResult.future();
  }

}
