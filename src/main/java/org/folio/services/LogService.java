package org.folio.services;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.LogCollection;

/**
 * Log Service interface, contains logic for accessing logs.
 */
public interface LogService {

  /**
   * Returns {@link LogCollection} by the input query
   *
   * @param query  query string to filter entities
   * @param offset starting index in a list of results
   * @param limit  maximum number of results to return
   */
  Future<LogCollection> getByQuery(String query, int offset, int limit);
}
