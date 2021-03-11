package org.folio.verticle.consumers.util;

import org.folio.DataImportEventPayload;
import org.folio.services.journal.JournalRecordMapperException;
import org.folio.services.journal.JournalService;

public interface SpecificEventHandler {

  void handle(JournalService journalService, DataImportEventPayload eventPayload, String tenantId)
    throws JournalRecordMapperException;

}
