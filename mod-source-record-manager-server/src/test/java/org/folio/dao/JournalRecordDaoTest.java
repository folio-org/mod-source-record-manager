package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.COMPLETED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.ERROR;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.CREATE;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.DELETE;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.MODIFY;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

@RunWith(VertxUnitRunner.class)
public class JournalRecordDaoTest extends AbstractRestTest {

  @Spy
  PostgresClientFactory postgresClientFactory = new PostgresClientFactory(Vertx.vertx());

  @InjectMocks
  JournalRecordDao journalRecordDao = new JournalRecordDaoImpl();

  @Before
  public void setUp(TestContext context) throws IOException {
    MockitoAnnotations.initMocks(this);
    super.setUp(context);
  }

  @Test
  public void shouldReturnSortedJournalRecordListByActionType(TestContext testContext) {
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    JournalRecord journalRecord1 = new JournalRecord()
      .withJobExecutionId(jobExec.getId())
      .withSourceRecordOrder(0)
      .withSourceId(UUID.randomUUID().toString())
      .withEntityType(JournalRecord.EntityType.MARC_BIBLIOGRAPHIC)
      .withEntityId(UUID.randomUUID().toString())
      .withActionType(CREATE)
      .withActionDate(new Date())
      .withActionStatus(COMPLETED);

    JournalRecord journalRecord2 = new JournalRecord()
      .withJobExecutionId(jobExec.getId())
      .withSourceRecordOrder(0)
      .withSourceId(UUID.randomUUID().toString())
      .withEntityType(JournalRecord.EntityType.INSTANCE)
      .withEntityId(UUID.randomUUID().toString())
      .withActionType(MODIFY)
      .withActionDate(new Date())
      .withActionStatus(COMPLETED);

    JournalRecord journalRecord3 = new JournalRecord()
      .withJobExecutionId(jobExec.getId())
      .withSourceRecordOrder(0)
      .withSourceId(UUID.randomUUID().toString())
      .withEntityType(JournalRecord.EntityType.INSTANCE)
      .withEntityId(UUID.randomUUID().toString())
      .withActionType(DELETE)
      .withActionDate(new Date())
      .withActionStatus(COMPLETED);

    Async async = testContext.async();
    Future<List<JournalRecord>> getFuture = journalRecordDao.save(journalRecord1, TENANT_ID)
      .compose(ar -> journalRecordDao.save(journalRecord2, TENANT_ID))
      .compose(ar -> journalRecordDao.save(journalRecord3, TENANT_ID))
      .compose(ar -> journalRecordDao.getByJobExecutionId(jobExec.getId(), "action_type", "asc", TENANT_ID));

    getFuture.setHandler(ar -> {
      testContext.verify(v -> {
        Assert.assertTrue(ar.succeeded());
        List<JournalRecord> journalRecords = ar.result();
        Assert.assertEquals(3, journalRecords.size());
        Assert.assertThat(journalRecords.get(0).getActionType(), lessThan(journalRecords.get(1).getActionType()));
        Assert.assertThat(journalRecords.get(1).getActionType(), lessThan(journalRecords.get(2).getActionType()));
      });
      async.complete();
    });
  }

  @Test
  public void shouldReturnSortedJournalRecordListByErrorMessage(TestContext testContext) {
    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    Assert.assertThat(createdJobExecutions.size(), is(1));
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

    getFuture.setHandler(ar -> {
      testContext.verify(v -> {
        Assert.assertTrue(ar.succeeded());
        List<JournalRecord> journalRecords = ar.result();
        Assert.assertEquals(3, journalRecords.size());
        Assert.assertThat(journalRecords.get(0).getError(), greaterThan(journalRecords.get(1).getError()));
        Assert.assertThat(journalRecords.get(1).getError(), greaterThan(journalRecords.get(2).getError()));
      });
      async.complete();
    });
  }
}
