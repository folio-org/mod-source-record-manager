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
  private static final String INSERT_SQL = "INSERT INTO %s.%s (handler_id, event_id) VALUES ($1, $2)";

  private static final String INSERT_EVENT_SQL = "SELECT save_event ($1, $2)";

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

 private void makeSaveCall(Promise<RowSet<Row>> promise, String query, String handlerId, String eventId, String tenantId) {
    try {
      pgClientFactory.createInstance(tenantId).execute(query, Tuple.of(handlerId, eventId), promise);
    } catch (Exception e) {
      LOGGER.error("Failed to save handlerId {} and eventId {} combination to table {}", handlerId,  eventId, EVENTS_PROCESSED_TABLE_NAME, e);
      promise.fail(e);
    }
  }

  private Future<Integer> getCounterValueFromRowSet(Promise<RowSet<Row>> promise) {
    return promise.future().map(resultSet -> resultSet.iterator().next().getInteger(0));
  }
}
