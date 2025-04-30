package org.folio.dao;

import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.COMPLETED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.ERROR;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.CREATE;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.DELETE;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.MODIFY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgException;
import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.persist.PostgresClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

@RunWith(VertxUnitRunner.class)
public class JournalRecordDaoTest extends AbstractRestTest {

  @Spy
  PostgresClientFactory postgresClientFactory = new PostgresClientFactory(Vertx.vertx());

  @Mock
  private PostgresClient pgClient;

  @InjectMocks
  JournalRecordDao journalRecordDao = new JournalRecordDaoImpl();

  @Before
  public void setUp(TestContext context) throws IOException {
    MockitoAnnotations.openMocks(this);
    super.setUp(context);
  }

  @Test
  public void shouldReturnSortedInstanceListByActionType(TestContext testContext) {
    shouldReturnSortedJournalRecordListByActionType(testContext, JournalRecord.EntityType.INSTANCE);
  }

  @Test
  public void shouldReturnSortedAuthorityListByActionType(TestContext testContext) {
    shouldReturnSortedJournalRecordListByActionType(testContext, JournalRecord.EntityType.MARC_AUTHORITY);
  }

  @Test
  public void shouldReturnSortedOrderListByActionType(TestContext testContext) {
    shouldReturnSortedJournalRecordListByActionType(testContext, JournalRecord.EntityType.ORDER);
  }

  private void shouldReturnSortedJournalRecordListByActionType(TestContext testContext,
                                                                 JournalRecord.EntityType entityType) {
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertEquals(1, createdJobExecutions.size());
    JobExecution jobExec = createdJobExecutions.get(0);

    JournalRecord journalRecord1 = new JournalRecord()
      .withJobExecutionId(jobExec.getId())
      .withSourceRecordOrder(0)
      .withSourceId(UUID.randomUUID().toString())
      .withEntityType(JournalRecord.EntityType.MARC_BIBLIOGRAPHIC)
      .withEntityId(UUID.randomUUID().toString())
      .withActionType(CREATE)
      .withActionDate(new Date())
      .withActionStatus(ERROR)
      .withError("Record creation error");

    JournalRecord journalRecord2 = new JournalRecord()
      .withJobExecutionId(jobExec.getId())
      .withSourceRecordOrder(0)
      .withSourceId(UUID.randomUUID().toString())
      .withEntityType(entityType)
      .withEntityId(UUID.randomUUID().toString())
      .withActionType(MODIFY)
      .withActionDate(new Date())
      .withActionStatus(ERROR)
      .withError(entityType.value() + " was not updated");

    JournalRecord journalRecord3 = new JournalRecord()
      .withJobExecutionId(jobExec.getId())
      .withSourceRecordOrder(0)
      .withSourceId(UUID.randomUUID().toString())
      .withEntityType(entityType)
      .withEntityId(UUID.randomUUID().toString())
      .withActionType(DELETE)
      .withActionDate(new Date())
      .withActionStatus(ERROR)
      .withError("No action taken");

    Async async = testContext.async();
    Future<List<JournalRecord>> getFuture = journalRecordDao.save(journalRecord1, TENANT_ID)
      .compose(ar -> journalRecordDao.save(journalRecord2, TENANT_ID))
      .compose(ar -> journalRecordDao.save(journalRecord3, TENANT_ID))
      .compose(ar -> journalRecordDao.getByJobExecutionId(jobExec.getId(), "action_type", "asc", TENANT_ID));

    getFuture.onComplete(ar -> {
      testContext.verify(v -> {
        assertTrue(ar.succeeded());
        List<JournalRecord> journalRecords = ar.result();
        assertEquals(3, journalRecords.size());
        assertThat(journalRecords.get(0).getActionType(), lessThan(journalRecords.get(1).getActionType()));
        assertThat(journalRecords.get(1).getActionType(), lessThan(journalRecords.get(2).getActionType()));
      });
      async.complete();
    });
  }

  @Test
  public void shouldRetryOnDeadlockAndSucceed(TestContext context) {
    Async async = context.async();
    // Setup mock behavior
    when(postgresClientFactory.createInstance(anyString())).thenReturn(pgClient);
    PgException deadlockException = new PgException("Deadlock", "ERROR", "40P01", "Deadlock detected");
    when(pgClient.execute(anyString(), anyList()))
      .thenReturn(Future.failedFuture(deadlockException)) // First attempt fails
      .thenReturn(Future.succeededFuture());             // Second attempt succeeds

    journalRecordDao.saveBatch(journalRecords(), TENANT_ID)
      .onComplete(context.asyncAssertSuccess(v -> {
        verify(pgClient, times(2)).execute(anyString(), anyList());
        async.complete();
      }));
  }

  private List<JournalRecord> journalRecords() {
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertEquals(1, createdJobExecutions.size());
    JobExecution jobExec = createdJobExecutions.get(0);

    JournalRecord journalRecord = new JournalRecord()
      .withId(UUID.randomUUID().toString())
      .withJobExecutionId(jobExec.getId())
      .withSourceRecordOrder(0)
      .withSourceId(UUID.randomUUID().toString())
      .withEntityType(JournalRecord.EntityType.MARC_BIBLIOGRAPHIC)
      .withEntityId(UUID.randomUUID().toString())
      .withActionType(CREATE)
      .withActionDate(new Date())
      .withActionStatus(COMPLETED);

    JournalRecord journalRecord2 = new JournalRecord()
      .withId(UUID.randomUUID().toString())
      .withJobExecutionId(jobExec.getId())
      .withSourceRecordOrder(0)
      .withSourceId(UUID.randomUUID().toString())
      .withEntityType(JournalRecord.EntityType.MARC_BIBLIOGRAPHIC)
      .withEntityId(UUID.randomUUID().toString())
      .withActionType(CREATE)
      .withActionDate(new Date())
      .withActionStatus(COMPLETED);

    return List.of(journalRecord, journalRecord2);
  }

  @Test
  public void shouldNotRetryOnOtherErrors(TestContext context) {
    Async async = context.async();
    when(postgresClientFactory.createInstance(anyString())).thenReturn(pgClient);
    // Setup non-deadlock error
    PgException otherError = new PgException("Constraint violation", "ERROR", "23505", "Unique violation");
    when(pgClient.execute(anyString(), anyList()))
      .thenReturn(Future.failedFuture(otherError));

    journalRecordDao.saveBatch(journalRecords(), TENANT_ID)
      .onComplete(context.asyncAssertFailure(throwable -> {
        verify(pgClient, times(1)).execute(anyString(), anyList());
        async.complete();
      }));
  }

  @Test
  public void shouldReturnSortedJournalRecordListByErrorMessage(TestContext testContext) {
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertEquals(1, createdJobExecutions.size());
    JobExecution jobExec = createdJobExecutions.get(0);

    JournalRecord journalRecord1 = new JournalRecord()
      .withJobExecutionId(jobExec.getId())
      .withSourceRecordOrder(0)
      .withSourceId(UUID.randomUUID().toString())
      .withEntityType(JournalRecord.EntityType.MARC_BIBLIOGRAPHIC)
      .withEntityId(UUID.randomUUID().toString())
      .withActionType(CREATE)
      .withActionDate(new Date())
      .withActionStatus(ERROR)
      .withError("Record creation error");

    JournalRecord journalRecord2 = new JournalRecord()
      .withJobExecutionId(jobExec.getId())
      .withSourceRecordOrder(0)
      .withSourceId(UUID.randomUUID().toString())
      .withEntityType(JournalRecord.EntityType.INSTANCE)
      .withEntityId(UUID.randomUUID().toString())
      .withActionType(MODIFY)
      .withActionDate(new Date())
      .withActionStatus(ERROR)
      .withError("Instance was not updated");

    JournalRecord journalRecord3 = new JournalRecord()
      .withJobExecutionId(jobExec.getId())
      .withSourceRecordOrder(0)
      .withSourceId(UUID.randomUUID().toString())
      .withEntityType(JournalRecord.EntityType.INSTANCE)
      .withEntityId(UUID.randomUUID().toString())
      .withActionType(DELETE)
      .withActionDate(new Date())
      .withActionStatus(ERROR)
      .withError("No action taken");

    Async async = testContext.async();
    Future<List<JournalRecord>> getFuture = journalRecordDao.save(journalRecord1, TENANT_ID)
      .compose(ar -> journalRecordDao.save(journalRecord2, TENANT_ID))
      .compose(ar -> journalRecordDao.save(journalRecord3, TENANT_ID))
      .compose(ar -> journalRecordDao.getByJobExecutionId(jobExec.getId(), "error", "desc", TENANT_ID));

    getFuture.onComplete(ar -> {
      testContext.verify(v -> {
        assertTrue(ar.succeeded());
        List<JournalRecord> journalRecords = ar.result();
        assertEquals(3, journalRecords.size());
        assertThat(journalRecords.get(0).getError(), greaterThan(journalRecords.get(1).getError()));
        assertThat(journalRecords.get(1).getError(), greaterThan(journalRecords.get(2).getError()));
      });
      async.complete();
    });
  }

  @Test
  public void shouldUpdateOnly2JournalRecordsByOrderIdAndJobExecutionId(TestContext testContext) {
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertEquals(1, createdJobExecutions.size());
    JobExecution jobExec = createdJobExecutions.get(0);
    String orderId = UUID.randomUUID().toString();

    JournalRecord journalRecord1 = new JournalRecord()
      .withJobExecutionId(jobExec.getId())
      .withSourceRecordOrder(0)
      .withSourceId(UUID.randomUUID().toString())
      .withEntityType(JournalRecord.EntityType.MARC_BIBLIOGRAPHIC)
      .withEntityId(UUID.randomUUID().toString())
      .withActionType(CREATE)
      .withActionDate(new Date())
      .withActionStatus(COMPLETED)
      .withOrderId(orderId);

    JournalRecord journalRecord2 = new JournalRecord()
      .withJobExecutionId(jobExec.getId())
      .withSourceRecordOrder(0)
      .withSourceId(UUID.randomUUID().toString())
      .withEntityType(JournalRecord.EntityType.INSTANCE)
      .withEntityId(UUID.randomUUID().toString())
      .withActionType(MODIFY)
      .withActionDate(new Date())
      .withActionStatus(COMPLETED)
      .withOrderId(orderId);

    JournalRecord journalRecord3 = new JournalRecord()
      .withJobExecutionId(jobExec.getId())
      .withSourceRecordOrder(0)
      .withSourceId(UUID.randomUUID().toString())
      .withEntityType(JournalRecord.EntityType.INSTANCE)
      .withEntityId(UUID.randomUUID().toString())
      .withActionType(DELETE)
      .withActionDate(new Date())
      .withActionStatus(COMPLETED)
      .withOrderId(UUID.randomUUID().toString())
      .withError("Testing Error");

    Future<Integer> updatedFuture = journalRecordDao.save(journalRecord1, TENANT_ID)
      .compose(ar -> journalRecordDao.save(journalRecord2, TENANT_ID))
      .compose(ar -> journalRecordDao.save(journalRecord3, TENANT_ID))
      .compose(ar -> journalRecordDao.updateErrorJournalRecordsByOrderIdAndJobExecution(jobExec.getId(), orderId,"Testing Error", TENANT_ID));


    Async async = testContext.async();
    updatedFuture.onComplete(ar -> {
      testContext.verify(v -> {
        assertTrue(ar.succeeded());
          int updatedCount = ar.result();
          assertEquals(2, updatedCount);
      });
      async.complete();
    });
  }
}
