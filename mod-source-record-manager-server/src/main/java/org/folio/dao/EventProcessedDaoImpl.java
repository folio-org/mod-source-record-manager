package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.PostgresClientFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import static java.lang.String.format;
import static org.folio.rest.persist.PostgresClient.convertToPsqlStandard;

@Repository
public class EventProcessedDaoImpl implements EventProcessedDao {

  private static final Logger LOGGER = LogManager.getLogger();

  public static final String EVENTS_PROCESSED_TABLE_NAME = "events_processed";
  public static final String FLOW_CONTROL_EVENTS_COUNTER_TABLE_NAME = "flow_control_events_counter";

  private static final String INSERT_SQL = "INSERT INTO %s.%s (handler_id, event_id) VALUES ($1, $2)";

  private static final String INSERT_EVENT_AND_DECREASE_FLOW_CONTROL_COUNTER_SQL = "SELECT save_event_and_decrease_flow_control_counter($1, $2)";

  private static final String DECREASE_COUNTER_SQL = "UPDATE %s.%s SET events_to_process = events_to_process - $1 RETURNING events_to_process";
  private static final String INCREASE_COUNTER_SQL = "UPDATE %s.%s SET events_to_process = events_to_process + $1 RETURNING events_to_process";
  private static final String SET_COUNTER_SQL = "UPDATE %s.%s SET events_to_process = $1 RETURNING events_to_process";

  private final PostgresClientFactory pgClientFactory;

  @Autowired
  public EventProcessedDaoImpl(PostgresClientFactory pgClientFactory) {
    this.pgClientFactory = pgClientFactory;
  }

  @Override
  public Future<RowSet<Row>> save(String handlerId, String eventId, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String query = format(INSERT_SQL, convertToPsqlStandard(tenantId), EVENTS_PROCESSED_TABLE_NAME);

    makeSaveCall(promise, query, handlerId, eventId, tenantId);

    return promise.future();
  }

  @Override
  public Future<Integer> saveAndDecreaseEventsToProcess(String handlerId, String eventId, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();

    makeSaveCall(promise, INSERT_EVENT_AND_DECREASE_FLOW_CONTROL_COUNTER_SQL, handlerId, eventId, tenantId);

    return getCounterValueFromRowSet(promise);
  }

  @Override
  public Future<Integer> decreaseEventsToProcess(String tenantId, Integer valueToDecrease) {
    return updateCounterValue(DECREASE_COUNTER_SQL, tenantId, valueToDecrease);
  }

  @Override
  public Future<Integer> increaseEventsToProcess(String tenantId, Integer valueToIncrease) {
    return updateCounterValue(INCREASE_COUNTER_SQL, tenantId, valueToIncrease);
  }

  @Override
  public Future<Integer> resetEventsToProcess(String tenantId) {
    return updateCounterValue(SET_COUNTER_SQL, tenantId, 0);
  }

  private void makeSaveCall(Promise<RowSet<Row>> promise, String query, String handlerId, String eventId, String tenantId) {
    try {
      pgClientFactory.createInstance(tenantId).execute(query, Tuple.of(handlerId, eventId), promise);
    } catch (Exception e) {
      LOGGER.error("Failed to save handlerId {} and eventId {} combination to table {}", handlerId,  eventId, EVENTS_PROCESSED_TABLE_NAME, e);
      promise.fail(e);
    }
  }

  private Future<Integer> updateCounterValue(String sqlQuery, String tenantId, Integer counterValue) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String query = String.format(sqlQuery, convertToPsqlStandard(tenantId), FLOW_CONTROL_EVENTS_COUNTER_TABLE_NAME);
      Tuple queryParams = Tuple.of(counterValue);
      pgClientFactory.createInstance(tenantId).execute(query, queryParams, promise);
    } catch (Exception e) {
      LOGGER.error("Failed to update counter value {} to table {}", counterValue, FLOW_CONTROL_EVENTS_COUNTER_TABLE_NAME, e);
      promise.fail(e);
    }
    return getCounterValueFromRowSet(promise);
  }

  private Future<Integer> getCounterValueFromRowSet(Promise<RowSet<Row>> promise) {
    return promise.future().map(resultSet -> resultSet.iterator().next().getInteger(0));
  }
}
