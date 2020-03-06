package org.folio.services;

import io.vertx.core.Future;
import org.folio.dataimport.util.OkapiConnectionParams;

/**
 * Event handling service
 */
public interface EventHandlingService {

  /**
   * Handles specified event content
   *
   * @param eventContent event content to handle
   * @param params       okapi connection parameters
   * @return future with true if the event was processed successfully
   */
  Future<Boolean> handle(String eventContent, OkapiConnectionParams params);
}
