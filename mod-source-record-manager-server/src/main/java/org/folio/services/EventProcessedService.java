package org.folio.services;

import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

public interface EventProcessedService {

  /**
   * Deduplication pattern implementation.
   * Collects deduplication data (information is event was already handled).
   * If events has not yet processed - future with Constraint violation exception will be returned.
   *
   * @param handlerId id of handler
   * @param eventId id of event
   * @param tenantId id of tenant
   * @return successful future if event has not been processed, or failed future otherwise
   */

  Future<RowSet<Row>> collectData(String handlerId, String eventId, String tenantId);

}
