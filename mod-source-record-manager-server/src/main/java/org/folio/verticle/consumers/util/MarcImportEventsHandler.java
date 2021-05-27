package org.folio.verticle.consumers.util;

import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.services.journal.JournalRecordMapperException;
import org.folio.services.journal.JournalService;
import org.folio.services.journal.JournalUtil;

import java.util.Optional;

public class MarcImportEventsHandler implements SpecificEventHandler {

  @Override
  public void handle(JournalService journalService, DataImportEventPayload eventPayload, String tenantId)
    throws JournalRecordMapperException {

    Optional<JournalParams> journalParamsOptional =
      JournalParams.JournalParamsEnum.getValue(eventPayload.getEventType()).getJournalParams(eventPayload);

    if (journalParamsOptional.isPresent()) {
      JournalParams journalParams = journalParamsOptional.get();
      JournalRecord journalRecord = JournalUtil.buildJournalRecordByEvent(eventPayload,
        journalParams.journalActionType, journalParams.journalEntityType, journalParams.journalActionStatus);

      journalService.save(JsonObject.mapFrom(journalRecord), tenantId);
    }
  }

}
