package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import org.folio.rest.jaxrs.model.Log;
import org.folio.rest.jaxrs.model.LogCollection;

import java.util.List;

/**
 * Current implementation for the LogDao uses data from text file.
 *
 * @see Log
 * @see LogDao
 */
public class LogDaoImpl implements LogDao {

  private static final String LOGS_STUB_PATH = "ramls/examples/logCollection.sample";

  private Vertx vertx;

  public LogDaoImpl(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public Future<List<Log>> getByQuery(String query, int offset, int limit) {
    Future<Buffer> future = Future.future();
    //TODO replace stub response
    vertx.fileSystem().readFile(LOGS_STUB_PATH, future.completer());
    return future.map(buffer -> new JsonObject(buffer).mapTo(LogCollection.class).getLogs()
    );
  }
}
