package org.folio.services;

import io.vertx.core.Future;
import io.vertx.pgclient.PgException;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.folio.dao.EventProcessedDao;
import org.folio.dataimport.util.exception.ConflictException;
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
  public Future<RowSet<Row>> collectData(String eventId, String handlerId, String tenantId) {
    return eventProcessedDao.save(eventId, handlerId, tenantId)
      .recover(throwable ->
        (throwable instanceof PgException && ((PgException) throwable).getCode().equals(UNIQUE_CONSTRAINT_VIOLATION_CODE)) ?
          Future.failedFuture(new ConflictException(String.format("Event with eventId=%s for handlerId=%s is already processed.", eventId, handlerId))) :
          Future.failedFuture(throwable));
  }
}
