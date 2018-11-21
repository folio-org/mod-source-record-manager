package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import org.folio.rest.jaxrs.model.Log;
import org.folio.rest.jaxrs.model.LogCollection;
import org.folio.rest.jaxrs.model.ResultInfo;
import org.folio.rest.persist.interfaces.Results;

/**
 * Current implementation for the LogDao uses {@link io.vertx.core.file.FileSystem#readFile} to access data
 * from text file.
 *
 * @see Log
 * @see LogDao
 */
public class LogDaoImpl implements LogDao {

  private static final String LOGS_STUB_PATH = "ramls/raml-storage/examples/mod-source-record-manager/logCollection.sample";

  private Vertx vertx;

  public LogDaoImpl(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public Future<Results<Log>> getByQuery(String query, int offset, int limit) {
    Future<Buffer> future = Future.future();
    //TODO replace stub response
    vertx.fileSystem().readFile(LOGS_STUB_PATH, future.completer());
    return future.map(buffer -> {
      LogCollection logs = new JsonObject(buffer).mapTo(LogCollection.class);
      Results<Log> results = new Results<>();
      results.setResults(logs.getLogs());
      results.setResultInfo(new ResultInfo().withTotalRecords(logs.getTotalRecords()));
      return results;
      }
    );
  }
}
