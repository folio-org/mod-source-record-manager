package org.folio.services.journal;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/**
 * Journal Service interface
 */
@ProxyGen
public interface JournalService {

  String JOURNAL_RECORD_SERVICE_ADDRESS = "journal-service.queue";  //NOSONAR

  static JournalService create() {
    return new JournalServiceImpl();
  }

  static JournalService createProxy(Vertx vertx) {
    return new JournalServiceVertxEBProxy(vertx, JOURNAL_RECORD_SERVICE_ADDRESS);
  }

  /**
   * Saves set of {@link org.folio.rest.jaxrs.model.JournalRecord} entities
   *
   * @param journalRecords json array that contains journalRecords as json objects
   * @param tenantId       tenant id
   * @throws IllegalArgumentException if the JournalRecord json from journalRecords cannot be mapped to JournalRecord entity
   */
  void save(JsonArray journalRecords, String tenantId);
}
