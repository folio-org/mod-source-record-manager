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

  /**
   * This method saves eventId and handler to deduplication table.
   * This method decreases counter value, that tracks how many events need to process.
   *
   * @param handlerId handler id of completed event
   * @param eventId event id
   * @param tenantId tenant id
   * @return future with counter value, or failed future if event already processed
   */
  Future<Integer> saveAndDecreaseEventsToProcess(String handlerId, String eventId, String tenantId);

  /**
   * This method decreases counter value, that tracks how many events need to process.
   *
   * @param valueToDecrease counter value to decrease
   * @param tenantId tenant id
   * @return future with counter value after update operation
   */
  Future<Integer> decreaseEventsToProcess(Integer valueToDecrease, String tenantId);

  /**
   * This method increases counter value, that tracks how many events need to process.
   *
   * @param valueToIncrease counter value to increase
   * @param tenantId tenant id
   * @return future with counter value after update operation
   */
  Future<Integer> increaseEventsToProcess(Integer valueToIncrease, String tenantId);
}
