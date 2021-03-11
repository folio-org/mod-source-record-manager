package org.folio.verticle.consumers.util;

import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.services.journal.JournalRecordMapperException;
import org.folio.services.journal.JournalService;
import org.folio.services.journal.JournalUtil;

public class MarcImportEventsHandler implements SpecificEventHandler {

  @Override
  public void handle(JournalService journalService, DataImportEventPayload eventPayload, String tenantId)
    throws JournalRecordMapperException {

    JournalParams journalParams =
      JournalParams.JournalParamsEnum.getValue(eventPayload.getEventType()).getJournalParams(eventPayload);

    JournalRecord journalRecord =
      JournalUtil.buildJournalRecordByEvent(eventPayload,
        journalParams.journalActionType, journalParams.journalEntityType, journalParams.journalActionStatus);

    journalService.save(JsonObject.mapFrom(journalRecord), tenantId);
  }

}
