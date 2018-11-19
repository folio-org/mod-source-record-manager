package org.folio.dao;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.Log;

import java.util.List;

/**
 * DAO interface for the Log entity.
 *
 * @see Log
 */
public interface LogDao {

  /**
   * Returns List of Log entities by the input query
   *
   * @param query  query string to filter entities
   * @param offset starting index in a list of results
   * @param limit  maximum number of results to return
   */
  Future<List<Log>> getByQuery(String query, int offset, int limit);
}
