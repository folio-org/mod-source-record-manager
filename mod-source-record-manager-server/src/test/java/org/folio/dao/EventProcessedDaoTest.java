package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgException;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.impl.AbstractRestTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Spy;

import java.io.IOException;
import java.util.UUID;

import static org.folio.dao.EventProcessedDaoImpl.FLOW_CONTROL_EVENTS_COUNTER_TABLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.folio.rest.persist.PostgresClient.convertToPsqlStandard;

@RunWith(VertxUnitRunner.class)
public class EventProcessedDaoTest extends AbstractRestTest {

  private String handlerId;
  private String eventId;

  @Spy
  private PostgresClientFactory postgresClientFactory = new PostgresClientFactory(Vertx.vertx());
  @InjectMocks
  private EventProcessedDaoImpl eventProcessedDao = new EventProcessedDaoImpl(postgresClientFactory);

  @Before
  public void setUp(TestContext context) throws IOException {
    super.setUp(context);
    handlerId = UUID.randomUUID().toString();
    eventId = UUID.randomUUID().toString();
    updateCounterToInitialValue(context);
  }

  @Test
  public void shouldSaveEventProcessed(TestContext context) {
    Async async = context.async();
    Future<RowSet<Row>> saveFuture = eventProcessedDao.save(handlerId, eventId, TENANT_ID);

    saveFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(1, ar.result().rowCount());
      async.complete();
    });
  }

  @Test
  public void shouldThrowConstraintViolation(TestContext context) {
    Async async = context.async();

    Future<RowSet<Row>> saveFuture = eventProcessedDao.save(handlerId, eventId, TENANT_ID);
    saveFuture.onComplete(ar -> {
      Future<RowSet<Row>> reSaveFuture = eventProcessedDao.save(handlerId, eventId, TENANT_ID);
      reSaveFuture.onComplete(re -> {
        context.assertTrue(re.failed());
        context.assertTrue(re.cause() instanceof  PgException);
        context.assertEquals("ERROR: duplicate key value violates unique constraint \"events_processed_pkey\" (23505)", re.cause().getMessage());
        async.complete();
      });
    });
  }

  @Test
  public void shouldSaveAndDecreaseCounter(TestContext context) {
    Async async = context.async();

    Future<Integer> saveFuture = eventProcessedDao.saveAndDecreaseEventsToProcess(handlerId, eventId, TENANT_ID);
    saveFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      assertEquals(Integer.valueOf(-1), ar.result());
      async.complete();
    });
  }

  @Test
  public void shouldThrowConstraintViolationWhenSavingAndDecreasingCounter(TestContext context) {
    Async async = context.async();

    Future<Integer> saveFuture = eventProcessedDao.saveAndDecreaseEventsToProcess(handlerId, eventId, TENANT_ID);
    saveFuture.onComplete(ar -> {
      Future<RowSet<Row>> reSaveFuture = eventProcessedDao.save(handlerId, eventId, TENANT_ID);
      reSaveFuture.onComplete(re -> {
        context.assertTrue(re.failed());
        context.assertTrue(re.cause() instanceof  PgException);
        context.assertEquals("ERROR: duplicate key value violates unique constraint \"events_processed_pkey\" (23505)", re.cause().getMessage());
        async.complete();
      });
    });
  }

  @Test
  public void shouldDecreaseCounter(TestContext context) {
    Async async = context.async();

    Future<Integer> future = eventProcessedDao.decreaseEventsToProcess(5, TENANT_ID);
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      assertEquals(Integer.valueOf(-5), ar.result());
      async.complete();
    });
  }

  @Test
  public void shouldIncreaseCounter(TestContext context) {
    Async async = context.async();

    Future<Integer> future = eventProcessedDao.increaseEventsToProcess(5, TENANT_ID);
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      assertEquals(Integer.valueOf(5), ar.result());
      async.complete();
    });
  }

  private void updateCounterToInitialValue(TestContext context) {
    Async async = context.async();
    String query = String.format("UPDATE %s.%s SET events_to_process = 0", convertToPsqlStandard(TENANT_ID), FLOW_CONTROL_EVENTS_COUNTER_TABLE_NAME);
    Future<RowSet<Row>> future = postgresClientFactory.createInstance(TENANT_ID).execute(query);
    future.onComplete(ar -> async.complete());
  }
}
