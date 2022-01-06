package org.folio.services;

import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

public interface EventProcessedService {

  /**
   * Collects information is event was handled.
   * Deduplication pattern implementation.
   *
   * @param handlerId id of handler
   * @param eventId id of event
   * @return future if the event not processed yet
   */

  Future<RowSet<Row>> collectData(String handlerId, String eventId, String tenantId);

}
