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

import static org.junit.Assert.assertEquals;

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

    Future<RowSet<Row>> saveFuture = eventProcessedDao.save(handlerId, eventId, TENANT_ID);
    saveFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      assertEquals(Integer.valueOf(-1), ar.result());
      async.complete();
    });
  }

  @Test
  public void shouldThrowConstraintViolationWhenSavingAndDecreasingCounter(TestContext context) {
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

}
