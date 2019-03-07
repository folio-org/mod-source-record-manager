package org.folio.dao;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.sql.UpdateResult;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dao.JobExecutionSourceChunkDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.interfaces.Results;
import org.folio.services.GenericHandlerAnswer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class JobExecutionSourceChunkDaoImplTest {

  private static final String TENANT_ID = "diku";

  private static final String TABLE_NAME = "job_execution_source_chunks";

  private JobExecutionSourceChunk jobExecutionSourceChunk = new JobExecutionSourceChunk()
    .withId("67dfac11-1caf-4470-9ad1-d533f6360bdd")
    .withJobExecutionId("zxsrt6hm-1caf-4470-9ad1-d533f6360bdd")
    .withLast(false)
    .withState(JobExecutionSourceChunk.State.COMPLETED)
    .withChunkSize(10)
    .withProcessedAmount(42);

  @Mock
  private PostgresClientFactory postgresClientFactory;

  @Mock
  private PostgresClient pgClient;

  @InjectMocks
  private JobExecutionSourceChunkDao jobExecutionSourceChunkDao = new JobExecutionSourceChunkDaoImpl();

  @Before
  public void setUp() {
    when(postgresClientFactory.createInstance(TENANT_ID))
      .thenReturn(pgClient);
  }

  @Test
  public void shouldReturnFutureWithEntityOnGettingById() {
    // given
    Results<Object> queryResults = new Results<>();
    queryResults.setResults(Collections.singletonList(jobExecutionSourceChunk));

    doAnswer(invocation -> {
      ((Handler) invocation.getArgument(5)).handle(Future.succeededFuture(queryResults));
      return null;
    })
      .when(pgClient).get(eq(TABLE_NAME), eq(JobExecutionSourceChunk.class), any(Criterion.class), eq(true), eq(false), any(Handler.class));
    // when
    jobExecutionSourceChunkDao.getById(jobExecutionSourceChunk.getId(), TENANT_ID)
    // then
      .setHandler(ar -> {
        Assert.assertTrue(ar.succeeded());
        Assert.assertTrue(ar.result().isPresent());
        JobExecutionSourceChunk receivedEntity = ar.result().get();
        Assert.assertEquals(jobExecutionSourceChunk.getId(), receivedEntity.getId());
        Assert.assertEquals(jobExecutionSourceChunk.getJobExecutionId(), receivedEntity.getJobExecutionId());
        Assert.assertEquals(jobExecutionSourceChunk.getLast(), receivedEntity.getLast());
        Assert.assertEquals(jobExecutionSourceChunk.getState(), receivedEntity.getState());
        Assert.assertEquals(jobExecutionSourceChunk.getChunkSize(), receivedEntity.getChunkSize());
        Assert.assertEquals(jobExecutionSourceChunk.getProcessedAmount(), receivedEntity.getProcessedAmount());

        verify(pgClient).get(eq(TABLE_NAME), eq(JobExecutionSourceChunk.class), any(Criterion.class), eq(true), eq(false), any(Handler.class));
      });
  }

  @Test
  public void shouldReturnFailedFutureWhenPgClientThrewExceptionOnGettingById() {
    // given
    doThrow(RuntimeException.class)
      .when(pgClient).get(eq(TABLE_NAME), eq(JobExecutionSourceChunk.class), any(Criterion.class), eq(true), eq(false), any(Handler.class));
    // when
    jobExecutionSourceChunkDao.getById(jobExecutionSourceChunk.getId(), TENANT_ID)
    // then
      .setHandler(ar -> {
        Assert.assertTrue(ar.failed());

        verify(pgClient).get(eq(TABLE_NAME), eq(JobExecutionSourceChunk.class), any(Criterion.class), eq(true), eq(false), any(Handler.class));
      });
  }

  @Test
  public void shouldReturnFutureWithTrueOnSuccessfulDeletionById() {
    // given
    int updatedRowsNumber = 1;
    UpdateResult updateResult = new UpdateResult();
    updateResult.setUpdated(updatedRowsNumber);

    doAnswer(invocation -> {
      ((Handler) invocation.getArgument(2)).handle(Future.succeededFuture(updateResult));
      return null;
    })
      .when(pgClient).delete(eq(TABLE_NAME), eq(jobExecutionSourceChunk.getId()), any(Handler.class));

    jobExecutionSourceChunkDao.delete(jobExecutionSourceChunk.getId(), TENANT_ID).setHandler(ar -> {
      Assert.assertTrue(ar.succeeded());
      Assert.assertEquals(true, ar.result());
      verify(pgClient).delete(eq(TABLE_NAME), eq(jobExecutionSourceChunk.getId()), any(Handler.class));
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenEntityWithSpecifiedIdNotFound() {
    // given
    int numberUpdatedRows = 0;
    UpdateResult sqlUpdateResult = when(mock(UpdateResult.class).getUpdated()).thenReturn(numberUpdatedRows).getMock();
    AsyncResult updateResult = mock(AsyncResult.class);
    when(updateResult.failed()).thenReturn(false);
    when(updateResult.result()).thenReturn(sqlUpdateResult);

    doAnswer(invocation -> {
      ((Handler) invocation.getArgument(4)).handle(updateResult);
      return null;
    })
      .when(pgClient).update(eq(TABLE_NAME), eq(jobExecutionSourceChunk), any(Criterion.class), eq(true), any(Handler.class));
    // when
    jobExecutionSourceChunkDao.update(jobExecutionSourceChunk, TENANT_ID)
    // then
    .setHandler(ar -> {
      Assert.assertTrue(ar.failed());
      verify(pgClient).update(eq(TABLE_NAME), eq(jobExecutionSourceChunk), any(Criterion.class), eq(true), any(Handler.class));;
    });
  }

  @Test
  public void shouldReturnFailedFutureWhenPgClientThrewException() {
    // given
    doThrow(RuntimeException.class)
      .when(pgClient).update(eq(TABLE_NAME), eq(jobExecutionSourceChunk), any(Criterion.class), eq(true), any(Handler.class));
    // when
    jobExecutionSourceChunkDao.update(jobExecutionSourceChunk, TENANT_ID)
    // then
      .setHandler(ar -> {
        Assert.assertTrue(ar.failed());
        verify(pgClient).update(eq(TABLE_NAME), eq(jobExecutionSourceChunk), any(Criterion.class), eq(true), any(Handler.class));
      });
  }
}
