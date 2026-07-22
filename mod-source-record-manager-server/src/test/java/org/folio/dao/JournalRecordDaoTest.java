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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgException;
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

@RunWith(VertxUnitRunner.class)
public class JournalRecordDaoTest extends AbstractRestTest {

  private JournalRecordDao journalRecordDao;

  @Before
  public void setUpDao() {
    journalRecordDao = new JournalRecordDaoImpl(new PostgresClientFactory(vertx));
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
    JobExecution jobExec = createdJobExecutions.getFirst();

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
        assertThat(journalRecords.getFirst().getActionType(), lessThan(journalRecords.get(1).getActionType()));
        assertThat(journalRecords.get(1).getActionType(), lessThan(journalRecords.get(2).getActionType()));
      });
      async.complete();
    });
  }

  @Test
  public void shouldRetryOnDeadlockAndSucceed(TestContext context) {
    Async async = context.async();
    // Setup mock behavior
    var factory = mock(PostgresClientFactory.class);
    var client = mock(PostgresClient.class);
    when(factory.createInstance(anyString())).thenReturn(client);
    when(factory.getVertx()).thenReturn(vertx);
    PgException deadlockException = new PgException("Deadlock", "ERROR", "40P01", "Deadlock detected");
    when(client.execute(anyString(), anyList()))
      .thenReturn(Future.failedFuture(deadlockException)) // First attempt fails
      .thenReturn(Future.succeededFuture());             // Second attempt succeeds

    new JournalRecordDaoImpl(factory).saveBatch(journalRecords(), TENANT_ID)
      .onComplete(context.asyncAssertSuccess(v -> {
        verify(client, times(2)).execute(anyString(), anyList());
        async.complete();
      }));
  }

  private List<JournalRecord> journalRecords() {
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertEquals(1, createdJobExecutions.size());
    JobExecution jobExec = createdJobExecutions.getFirst();

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
    var factory = mock(PostgresClientFactory.class);
    var client = mock(PostgresClient.class);
    when(factory.createInstance(anyString())).thenReturn(client);
    // Setup non-deadlock error
    PgException otherError = new PgException("Constraint violation", "ERROR", "23505", "Unique violation");
    when(client.execute(anyString(), anyList()))
      .thenReturn(Future.failedFuture(otherError));

    new JournalRecordDaoImpl(factory).saveBatch(journalRecords(), TENANT_ID)
      .onComplete(context.asyncAssertFailure(throwable -> {
        verify(client, times(1)).execute(anyString(), anyList());
        async.complete();
      }));
  }

  @Test
  public void shouldReturnSortedJournalRecordListByErrorMessage(TestContext testContext) {
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertEquals(1, createdJobExecutions.size());
    JobExecution jobExec = createdJobExecutions.getFirst();

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
        assertThat(journalRecords.getFirst().getError(), greaterThan(journalRecords.get(1).getError()));
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
    JobExecution jobExec = createdJobExecutions.getFirst();
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
