package org.folio.services;

import io.vertx.core.Future;
import io.vertx.pgclient.PgException;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.folio.dao.EventProcessedDao;
import org.folio.kafka.exception.DuplicateEventException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import static org.folio.services.AbstractChunkProcessingService.UNIQUE_CONSTRAINT_VIOLATION_CODE;

@Service("eventProcessedService")
public class EventProcessedServiceImpl implements EventProcessedService {

  private EventProcessedDao eventProcessedDao;

  @Autowired
  public EventProcessedServiceImpl(EventProcessedDao eventProcessedDao) {
    this.eventProcessedDao = eventProcessedDao;
  }

  @Override
  public Future<RowSet<Row>> collectData(String handlerId, String eventId, String tenantId) {
    return eventProcessedDao.save(handlerId, eventId, tenantId)
      .recover(throwable -> handleFailures(throwable, handlerId, eventId));
  }

  @Override
  public Future<Integer> collectDataAndDecreaseEventsToProcess(String handlerId, String eventId, String tenantId) {
    return eventProcessedDao.saveAndDecreaseEventsToProcess(handlerId, eventId, tenantId)
      .recover(throwable -> handleFailures(throwable, handlerId, eventId));
  }

  private <T> Future<T> handleFailures(Throwable throwable, String handlerId, String eventId) {
    return (throwable instanceof PgException && ((PgException) throwable).getCode().equals(UNIQUE_CONSTRAINT_VIOLATION_CODE)) ?
        Future.failedFuture(new DuplicateEventException(String.format("Event with eventId=%s for handlerId=%s is already processed.", eventId, handlerId))) :
        Future.failedFuture(throwable);
  }

  @Override
  public Future<Integer> increaseEventsToProcess(String tenantId, Integer valueToIncrease) {
    return eventProcessedDao.increaseEventsToProcess(tenantId, valueToIncrease);
  }

  @Override
  public Future<Integer> decreaseEventsToProcess(String tenantId, Integer valueToDecrease) {
    return eventProcessedDao.decreaseEventsToProcess(tenantId, valueToDecrease);
  }

  @Override
  public Future<Integer> resetEventsToProcess(String tenantId) {
    return eventProcessedDao.resetEventsToProcess(tenantId);
  }
}
