package org.folio.dao.util;

import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.persist.Conn;
import org.folio.rest.persist.PostgresClient;

import java.util.function.Function;

/**
 * Util class containing helper methods for interacting with db
 */
public final class DbUtil {

  private DbUtil() {
  }

  private static final Logger LOGGER = LogManager.getLogger();

  /**
   * Executes passed action in transaction
   *
   * @param postgresClient Postgres Client
   * @param action         action that needs to be executed in transaction
   * @param <T>            result type returned from the action
   * @return future with action result if succeeded or failed future
   */
  public static <T> Future<T> executeInTransaction(PostgresClient postgresClient,
                                                   Function<Conn, Future<T>> action) {
    return postgresClient.withTrans(action)
      .onFailure(e -> LOGGER.warn("executeInTransaction:: Error executing action in transaction", e));
  }
}
