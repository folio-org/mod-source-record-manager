package org.folio.dao;

import io.vertx.core.Future;
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

  private final PostgresClientFactory pgClientFactory;

  @Autowired
  public EventProcessedDaoImpl(PostgresClientFactory pgClientFactory) {
    this.pgClientFactory = pgClientFactory;
  }

  @Override
  public Future<RowSet<Row>> save(String handlerId, String eventId, String tenantId) {
    String query = format(INSERT_SQL, convertToPsqlStandard(tenantId), EVENTS_PROCESSED_TABLE_NAME);
    return makeSaveCall(query, handlerId, eventId, tenantId);
  }

 private Future<RowSet<Row>> makeSaveCall(String query, String handlerId, String eventId, String tenantId) {
    try {
      return pgClientFactory.createInstance(tenantId).execute(query, Tuple.of(handlerId, eventId));
    } catch (Exception e) {
      LOGGER.warn("makeSaveCall:: Failed to save handlerId {} and eventId {} combination to table {}", handlerId,  eventId, EVENTS_PROCESSED_TABLE_NAME, e);
      return Future.failedFuture(e);
    }
 }

}
