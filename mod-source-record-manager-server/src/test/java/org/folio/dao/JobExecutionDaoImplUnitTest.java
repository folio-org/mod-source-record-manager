package org.folio.dao;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.util.UUID;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.dataimport.util.test.GenericHandlerAnswer;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class JobExecutionDaoImplUnitTest {

  private static final String TENANT_ID = "diku";

  private static final String TABLE_NAME = "job_executions";

  private JobExecution jobExecution = new JobExecution()
    .withId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
    .withHrId(1000)
    .withParentJobId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
    .withSubordinationType(JobExecution.SubordinationType.PARENT_SINGLE)
    .withStatus(JobExecution.Status.NEW)
    .withUiStatus(JobExecution.UiStatus.INITIALIZATION)
    .withSourcePath("importMarc.mrc")
    .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"))
    .withUserId(UUID.randomUUID().toString());

  @Mock
  private PostgresClientFactory postgresClientFactory;

  @Mock
  private PostgresClient pgClient;

  @InjectMocks
  private JobExecutionDao jobExecutionDao = new JobExecutionDaoImpl();

  @Before
  public void setUp() {
    when(postgresClientFactory.createInstance(TENANT_ID))
      .thenReturn(pgClient);
  }

  @Test
  public void shouldReturnFutureWithUpdatedEntity() {
    // given
    int updatedRowsNumber = 1;
    RowSet<Row> sqlUpdateResult = when(mock(RowSet.class).rowCount()).thenReturn(updatedRowsNumber).getMock();
    AsyncResult updateResult = mock(AsyncResult.class);
    when(updateResult.failed()).thenReturn(false);
    when(updateResult.result()).thenReturn(sqlUpdateResult);

    doAnswer(new GenericHandlerAnswer<>(updateResult, 4))
      .when(pgClient).update(eq(TABLE_NAME), eq(jobExecution), any(Criterion.class), eq(true), any(Handler.class));

    // when
    jobExecutionDao.updateJobExecution(jobExecution, TENANT_ID)

      // then
      .onComplete(ar -> {
        Assert.assertTrue(ar.succeeded());
        Assert.assertEquals(jobExecution.getId(), ar.result().getId());
        Assert.assertEquals(jobExecution.getHrId(), ar.result().getHrId());
        Assert.assertEquals(jobExecution.getParentJobId(), ar.result().getParentJobId());
        Assert.assertEquals(jobExecution.getSubordinationType(), ar.result().getSubordinationType());
        Assert.assertEquals(jobExecution.getStatus(), ar.result().getStatus());
        Assert.assertEquals(jobExecution.getUiStatus(), ar.result().getUiStatus());
        Assert.assertEquals(jobExecution.getSourcePath(), ar.result().getSourcePath());
        Assert.assertEquals(jobExecution.getUserId(), ar.result().getUserId());
        Assert.assertEquals(jobExecution.getJobProfileInfo().getId(), ar.result().getJobProfileInfo().getId());
        Assert.assertEquals(jobExecution.getJobProfileInfo().getName(), ar.result().getJobProfileInfo().getName());

        verify(pgClient).update(eq(TABLE_NAME), eq(jobExecution), any(Criterion.class), eq(true), any(Handler.class));
      });
  }

  @Test
  public void shouldReturnFailedFutureWhenEntityWithSpecifiedIdNotFound() {
    // given
    int numberUpdatedRows = 0;
    RowSet<Row> sqlUpdateResult = when(mock(RowSet.class).rowCount()).thenReturn(numberUpdatedRows).getMock();
    AsyncResult updateResult = mock(AsyncResult.class);
    when(updateResult.failed()).thenReturn(false);
    when(updateResult.result()).thenReturn(sqlUpdateResult);

    doAnswer(new GenericHandlerAnswer<>(updateResult, 4))
      .when(pgClient).update(eq(TABLE_NAME), eq(jobExecution), any(Criterion.class), eq(true), any(Handler.class));
    // when
    jobExecutionDao.updateJobExecution(jobExecution, TENANT_ID)
      // then
      .onComplete(ar -> {
        Assert.assertTrue(ar.failed());
        verify(pgClient).update(eq(TABLE_NAME), eq(jobExecution), any(Criterion.class), eq(true), any(Handler.class));
      });
  }

  @Test
  public void shouldReturnFailedFutureWhenPgClientReturnedFailedFuture() {
    // given
    AsyncResult updateResult = mock(AsyncResult.class);
    when(updateResult.failed()).thenReturn(true);

    doAnswer(new GenericHandlerAnswer<>(updateResult, 4))
      .when(pgClient).update(eq(TABLE_NAME), eq(jobExecution), any(Criterion.class), eq(true), any(Handler.class));
    // when
    jobExecutionDao.updateJobExecution(jobExecution, TENANT_ID)
      // then
      .onComplete(ar -> {
        Assert.assertTrue(ar.failed());
        verify(pgClient).update(eq(TABLE_NAME), eq(jobExecution), any(Criterion.class), eq(true), any(Handler.class));
      });
  }

  @Test
  public void shouldReturnFailedFutureWhenPgClientThrewException() {
    // given
    doThrow(RuntimeException.class)
      .when(pgClient).update(eq(TABLE_NAME), eq(jobExecution), any(Criterion.class), eq(true), any(Handler.class));
    // when
    jobExecutionDao.updateJobExecution(jobExecution, TENANT_ID)
      // then
      .onComplete(ar -> {
        Assert.assertTrue(ar.failed());
        verify(pgClient).update(eq(TABLE_NAME), eq(jobExecution), any(Criterion.class), eq(true), any(Handler.class));
      });
  }
}
