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

  /**
   * Deduplication pattern implementation.
   * Collects deduplication data (information is event was already handled).
   * Decreases events to process counter by one in case if event has not yet processed.
   * If event has been already processed - future with Constraint violation exception will be returned, counter value will remain without changes.
   *
   * @param handlerId
   * @param eventId   id of event
   * @param tenantId  id of tenant
   * @return successful future with state of counter if event has not been processed, or failed future otherwise
   */
  Future<Integer> collectDataAndDecreaseEventsToProcess(String handlerId, String eventId, String tenantId);

  /**
   * Increases events needed to process counter.
   *
   * @param valueToIncrease value to increase
   * @param tenantId tenant id
   * @return future with actual state of counter
   */
  Future<Integer> increaseEventsToProcess(Integer valueToIncrease, String tenantId);

  /**
   * Decreases events needed to process counter.
   *
   * @param valueToDecrease value to decrease
   * @param tenantId tenant id
   * @return future with actual state of counter
   */
  Future<Integer> decreaseEventsToProcess(Integer valueToDecrease, String tenantId);

}
