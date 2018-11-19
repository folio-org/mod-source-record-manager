package org.folio.dao;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.Log;
import org.folio.rest.persist.interfaces.Results;

/**
 * DAO interface for the Log entity.
 *
 * @see Log
 */
public interface LogDao {

  /**
   * Returns Results parametrized by Log entities
   *
   * @param query  query string to filter entities
   * @param offset starting index in a list of results
   * @param limit  maximum number of results to return
   */
  Future<Results<Log>> getByQuery(String query, int offset, int limit);
}
