package org.folio.verticle.consumers.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.services.journal.InvoiceUtil;
import org.folio.services.journal.JournalRecordMapperException;
import org.folio.services.journal.JournalService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class InvoiceImportEventHandler implements SpecificEventHandler {

  @Override
  public void handle(JournalService journalService, DataImportEventPayload eventPayload, String tenantId)
    throws JournalRecordMapperException, JsonProcessingException {
    List<JournalRecord> journalRecords = InvoiceUtil.buildJournalRecordByEvent(eventPayload);
    List<JsonObject> jsonObjects = new ArrayList<>();
    journalRecords.forEach(journalRecord -> jsonObjects.add(JsonObject.mapFrom(journalRecord)));
    journalService.saveBatch(new JsonArray(jsonObjects), tenantId);
  }

  @Override
  public Future<Collection<JournalRecord>> transform(JournalService journalService, DataImportEventPayload eventPayload, String tenantId) throws JournalRecordMapperException, JsonProcessingException {
    List<JournalRecord> journalRecords = InvoiceUtil.buildJournalRecordByEvent(eventPayload);
    return Future.succeededFuture(journalRecords);
  }
}
