package org.folio.services;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * JournalRecord Service interface
 */
@ProxyGen
public interface JournalRecordService {

  String JOURNAL_RECORD_SERVICE_ADDRESS = "journal-service.queue";  //NOSONAR

  static JournalRecordService createProxy(Vertx vertx) {
    return new JournalRecordServiceVertxEBProxy(vertx, JOURNAL_RECORD_SERVICE_ADDRESS);
  }

  /**
   * Saves {@link org.folio.rest.jaxrs.model.JournalRecord} entity
   *
   * @param jsonJournalRecord journalRecord as json object
   * @param tenantId          tenant id
   * @throws IllegalArgumentException if the jsonJournalRecord cannot be mapped to JournalRecord entity
   */
  void save(JsonObject jsonJournalRecord, String tenantId);
}
