package org.folio.services;

import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.folio.dao.EventProcessedDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("eventProcessedService")
public class EventProcessedServiceImpl implements EventProcessedService {

  private EventProcessedDao eventProcessedDao;

  @Autowired
  public EventProcessedServiceImpl(EventProcessedDao eventProcessedDao) {
    this.eventProcessedDao = eventProcessedDao;
  }

  @Override
  public Future<RowSet<Row>> collectData(String eventId, String handlerId, String tenantId) {
    return eventProcessedDao.save(eventId, handlerId, tenantId);
  }
}
