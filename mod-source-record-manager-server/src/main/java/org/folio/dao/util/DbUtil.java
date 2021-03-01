package org.folio.dao.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.SQLConnection;

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
                                                   Function<AsyncResult<SQLConnection>, Future<T>> action) {
    Promise<T> promise = Promise.promise();
    Promise<SQLConnection> tx = Promise.promise();
    Future.succeededFuture()
      .compose(v -> {
        postgresClient.startTx(tx);
        return tx.future();
      })
      .compose(v -> action.apply(tx.future()))
      .onComplete(result -> {
        if (result.succeeded()) {
          postgresClient.endTx(tx.future(), endTx -> promise.complete(result.result()));
        } else {
          postgresClient.rollbackTx(tx.future(), r -> {
            LOGGER.error("Rollback transaction", result.cause());
            promise.fail(result.cause());
          });
        }
      });
    return promise.future();
  }
}
