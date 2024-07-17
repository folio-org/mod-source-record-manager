package org.folio.verticle.consumers.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.vertx.core.Future;
import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.services.journal.BatchJournalService;
import org.folio.services.journal.JournalRecordMapperException;
import org.folio.services.journal.JournalService;

import java.util.Collection;

public interface SpecificEventHandler {

  void handle(JournalService journalService, DataImportEventPayload eventPayload, String tenantId)
    throws JournalRecordMapperException, JsonProcessingException;

  Future<Collection<JournalRecord>> transform(BatchJournalService journalService, DataImportEventPayload eventPayload, String tenantId)
    throws JournalRecordMapperException, JsonProcessingException;

}
