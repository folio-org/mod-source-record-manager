package org.folio.dao.util;

import io.vertx.core.Future;
import lombok.extern.log4j.Log4j2;
import org.folio.rest.persist.Conn;
import org.folio.rest.persist.PostgresClient;

import java.time.Instant;
import java.util.Date;
import java.util.function.Function;

/**
 * Util class containing helper methods for interacting with db
 */
@Log4j2
public final class DbUtil {

  private DbUtil() {
  }

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
      .onFailure(e -> log.warn("executeInTransaction:: Error executing action in transaction", e));
  }

  /**
   * Converts a date read from a jsonb column. Depending on the serializer that wrote the row, a date is stored
   * either as epoch millis or as an ISO-8601 string.
   *
   * @param value jsonb value: a {@link Number} of epoch millis or an ISO-8601 instant string
   * @return the date
   */
  public static Date toDate(Object value) {
    if (value instanceof Number number) {
      return new Date(number.longValue());
    }
    return Date.from(Instant.parse(String.valueOf(value)));
  }
}
