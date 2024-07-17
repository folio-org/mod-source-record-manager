package org.folio.services.journal;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.folio.rest.jaxrs.model.JournalRecord;

import java.util.Collection;

/**
 * Batch Journal Service interface
 */
public interface BatchJournalService {
  /**
   * Saves set of {@link JournalRecord} entities
   *
   * @param journalRecords collection of journal records
   * @param tenantId       tenant id
   * @param resultHandler  handler for a response
   * @throws IllegalArgumentException if the JournalRecord json from journalRecords cannot be mapped to JournalRecord entity
   */
  void saveBatchWithResponse(Collection<JournalRecord> journalRecords, String tenantId, Handler<AsyncResult<Void>> resultHandler);

  JournalService getJournalService();
}
