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

  private static final String TABLE_NAME = "events_processed";

  private static final String INSERT_SQL = "INSERT INTO %s.%s (handler_id, event_id) VALUES ($1, $2)";

  private PostgresClientFactory pgClientFactory;

  @Autowired
  public EventProcessedDaoImpl(PostgresClientFactory pgClientFactory) {
    this.pgClientFactory = pgClientFactory;
  }

  @Override
  public Future<RowSet<Row>> save(String eventId, String handlerId, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String query = format(INSERT_SQL, convertToPsqlStandard(tenantId), TABLE_NAME);
      pgClientFactory.createInstance(tenantId).execute(query, Tuple.of(eventId, handlerId), promise);
    } catch (Exception e) {
      LOGGER.error("Failed to save eventId {} to {} with handlerId: {}", eventId, TABLE_NAME, handlerId, e);
      promise.fail(e);
    }
    return promise.future();
  }
}
