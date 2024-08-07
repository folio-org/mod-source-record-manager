package org.folio.services.journal;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.folio.rest.jaxrs.model.JournalRecord;

import java.util.Collection;

public interface BatchJournalService {
  /**
   * Saves set of {@link JournalRecord} entities
   */
  void saveBatchWithResponse(Collection<JournalRecord> journalRecords, String tenantId, Handler<AsyncResult<Void>> resultHandler);

  JournalService getJournalService();
}
