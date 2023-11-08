package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

public interface EventProcessedDao {

  /**
   * This method saves eventId and handlerId to deduplication table.
   *
   * @param handlerId handler id
   * @param eventId event id
   * @param tenantId tenant id
   * @return successful future if event has not been processed already, or failed future otherwise
   */
  Future<RowSet<Row>> save(String handlerId, String eventId, String tenantId);

}
