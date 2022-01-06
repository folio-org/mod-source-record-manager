package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

public interface EventProcessedDao {

  /**
   * Saves eventId and handlerId to database
   *
   * @param handlerId handler id
   * @param eventId event id
   * @param tenantId tenant id
   * @return future if the event not processed yet
   */
  Future<RowSet<Row>> save(String handlerId, String eventId, String tenantId);
}
