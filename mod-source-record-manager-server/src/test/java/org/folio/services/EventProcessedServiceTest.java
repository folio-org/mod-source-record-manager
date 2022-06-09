package org.folio.services;

import io.vertx.core.Future;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgException;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.folio.dao.EventProcessedDao;
import org.folio.kafka.exception.DuplicateEventException;
import org.junit.Before;
import org.junit.Test;

import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.UUID;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.folio.services.AbstractChunkProcessingService.UNIQUE_CONSTRAINT_VIOLATION_CODE;
import static org.junit.jupiter.api.Assertions.assertTrue;

@RunWith(VertxUnitRunner.class)
public class EventProcessedServiceTest {

  private static final String EVENT_ID = UUID.randomUUID().toString();
  private static final String HANDLER_ID = UUID.randomUUID().toString();
  private static final String TENANT_ID = "diku";

  @Mock
  private EventProcessedDao eventProcessedDao;

  @InjectMocks
  private EventProcessedService eventProcessedService = new EventProcessedServiceImpl(eventProcessedDao);

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void shouldCallDaoForSuccessfulCase() {
    when(eventProcessedDao.save(HANDLER_ID, EVENT_ID, TENANT_ID)).thenReturn(Future.succeededFuture());

    eventProcessedService.collectData(HANDLER_ID, EVENT_ID, TENANT_ID);

    verify(eventProcessedDao).save(HANDLER_ID, EVENT_ID, TENANT_ID);
  }

  @Test
  public void shouldCallDaoForSuccessfulCaseForSaveAndUpdateCounter() {
    when(eventProcessedDao.saveAndDecreaseEventsToProcess(HANDLER_ID, EVENT_ID, TENANT_ID)).thenReturn(Future.succeededFuture());

    eventProcessedService.collectDataAndDecreaseEventsToProcess(HANDLER_ID, EVENT_ID, TENANT_ID);

    verify(eventProcessedDao).saveAndDecreaseEventsToProcess(HANDLER_ID, EVENT_ID, TENANT_ID);
  }

  @Test
  public void shouldReturnFailedFutureWithDuplicateExceptionWhenConstraintViolation() {
    when(eventProcessedDao.save(HANDLER_ID, EVENT_ID, TENANT_ID))
      .thenReturn(Future.failedFuture(new PgException("DB error", "ERROR", UNIQUE_CONSTRAINT_VIOLATION_CODE, "ConstrainViolation")));

    Future<RowSet<Row>> future = eventProcessedService.collectData(HANDLER_ID, EVENT_ID, TENANT_ID);

    verify(eventProcessedDao).save(HANDLER_ID, EVENT_ID, TENANT_ID);
    assertTrue(future.failed());
    assertTrue(future.cause() instanceof DuplicateEventException);
  }

  @Test
  public void shouldReturnFailedFutureWithDuplicateExceptionForSaveAndUpdateCounter() {
    when(eventProcessedDao.saveAndDecreaseEventsToProcess(HANDLER_ID, EVENT_ID, TENANT_ID))
      .thenReturn(Future.failedFuture(new PgException("DB error", "ERROR", UNIQUE_CONSTRAINT_VIOLATION_CODE, "ConstrainViolation")));

    Future<Integer> future = eventProcessedService.collectDataAndDecreaseEventsToProcess(HANDLER_ID, EVENT_ID, TENANT_ID);

    verify(eventProcessedDao).saveAndDecreaseEventsToProcess(HANDLER_ID, EVENT_ID, TENANT_ID);
    assertTrue(future.failed());
    assertTrue(future.cause() instanceof DuplicateEventException);
  }

  @Test
  public void shouldReturnFailedFutureWhenDbFails() {
    when(eventProcessedDao.save(HANDLER_ID, EVENT_ID, TENANT_ID))
      .thenReturn(Future.failedFuture(new PgException("DB error", "ERROR", "ERROR_CODE", "DB is unavailable")));

    Future<RowSet<Row>> future = eventProcessedService.collectData(HANDLER_ID, EVENT_ID, TENANT_ID);

    verify(eventProcessedDao).save(HANDLER_ID, EVENT_ID, TENANT_ID);
    assertTrue(future.failed());
    assertTrue(future.cause() instanceof PgException);

  }

  @Test
  public void shouldReturnFailedFutureWhenDbFailsForSaveAndUpdateCounter() {
    when(eventProcessedDao.saveAndDecreaseEventsToProcess(HANDLER_ID, EVENT_ID, TENANT_ID))
      .thenReturn(Future.failedFuture(new PgException("DB error", "ERROR", "ERROR_CODE", "DB is unavailable")));

    Future<Integer> future = eventProcessedService.collectDataAndDecreaseEventsToProcess(HANDLER_ID, EVENT_ID, TENANT_ID);
    verify(eventProcessedDao).saveAndDecreaseEventsToProcess(HANDLER_ID, EVENT_ID, TENANT_ID);
    assertTrue(future.failed());
    assertTrue(future.cause() instanceof PgException);
  }
}
