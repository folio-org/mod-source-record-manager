package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.folio.rest.jaxrs.model.JobExecution;

public interface EventProcessedDao {

  /**
   * Saves eventId and handlerId to database
   *
   * @param eventId event id
   * @param handlerId handler id
   * @param tenantId tenant id
   * @return future if the event not processed yet
   */
  Future<RowSet<Row>> save(String eventId, String handlerId, String tenantId);
}
