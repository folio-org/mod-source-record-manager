package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import org.folio.dao.LogDao;
import org.folio.dao.LogDaoImpl;
import org.folio.rest.jaxrs.model.Log;
import org.folio.rest.jaxrs.model.LogCollection;

/**
 * Implementation of the LogService, calls LogDao to access Log metadata.
 *
 * @see LogService
 * @see LogDao
 * @see Log
 */
public class LogServiceImpl implements LogService {

  private LogDao dao;

  public LogServiceImpl(Vertx vertx) {
    this.dao = new LogDaoImpl(vertx);
  }

  @Override
  public Future<LogCollection> getByQuery(String query, int offset, int limit) {
    return dao.getByQuery(query, offset, limit).map(results -> new LogCollection().withLogs(results.getResults()));
  }
}
