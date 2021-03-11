package org.folio.verticle.consumers.util;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.services.journal.InvoiceUtil;
import org.folio.services.journal.JournalRecordMapperException;
import org.folio.services.journal.JournalService;

import java.util.ArrayList;
import java.util.List;

public class InvoiceImportEventHandler implements SpecificEventHandler {

  @Override
  public void handle(JournalService journalService, DataImportEventPayload eventPayload, String tenantId)
    throws JournalRecordMapperException {
    List<JournalRecord> journalRecords = InvoiceUtil.buildJournalRecordByEvent(eventPayload);
    List<JsonObject> jsonObjects = new ArrayList<>();
    journalRecords.forEach(journalRecord -> jsonObjects.add(JsonObject.mapFrom(journalRecord)));
    journalService.saveBatch(new JsonArray(jsonObjects), tenantId);
  }
}
