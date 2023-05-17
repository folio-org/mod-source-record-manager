
package org.folio.dao.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.persist.PostgresClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static org.folio.rest.persist.PostgresClient.convertToPsqlStandard;

@Component
public class PostgresClientFactory {

  private static final Logger LOGGER = LogManager.getLogger();

  private Vertx vertx;

  public PostgresClientFactory(@Autowired Vertx vertx) {
    this.vertx = vertx;
  }

  /**
   * Creates instance of Postgres Client
   *
   * @param tenantId tenant id
   * @return Postgres Client
   */
  public PostgresClient createInstance(String tenantId) {
    LOGGER.warn("createInstance:: getPostgresClient");
    PostgresClient postgresClient = PostgresClient.getInstance(vertx, tenantId);
    getUsedConnectionNumber(postgresClient).onSuccess(res -> LOGGER.warn("active connection number = {}", res));
    return postgresClient;
  }

  private Future<Integer> getUsedConnectionNumber(PostgresClient postgresClient) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      postgresClient.execute("select count(state) from pg_stat_activity where state = 'active';", promise);
    } catch (Exception e) {
      LOGGER.warn("updateCounterValue:: Failed to get counter value to table", e);
      promise.fail(e);
    }
    return getCounterValueFromRowSet(promise);
  }

  private Future<Integer> getCounterValueFromRowSet(Promise<RowSet<Row>> promise) {
    return promise.future().map(resultSet -> resultSet.iterator().next().getInteger(0));
  }

}
