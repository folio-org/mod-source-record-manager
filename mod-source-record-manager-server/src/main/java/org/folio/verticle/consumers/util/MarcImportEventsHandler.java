package org.folio.verticle.consumers.util;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.ActionProfile;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.journal.JournalRecordMapperException;
import org.folio.services.journal.JournalService;
import org.folio.services.journal.JournalUtil;
import org.folio.services.util.ParsedRecordUtil;

import java.util.List;
import java.util.Optional;

import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;

public class MarcImportEventsHandler implements SpecificEventHandler {

  private List<String> titleSubfields = List.of("a", "n", "p", "b", "c", "f", "g", "h", "k", "s");
  public static final String MARC_BIB_CREATED_LOG_EVENT = "DI_LOG_SRS_MARC_BIB_RECORD_CREATED";

  @Override
  public void handle(JournalService journalService, DataImportEventPayload eventPayload, String tenantId)
    throws JournalRecordMapperException {

    Optional<JournalParams> journalParamsOptional =
      JournalParams.JournalParamsEnum.getValue(eventPayload.getEventType()).getJournalParams(eventPayload);

    if (journalParamsOptional.isPresent()) {
      JournalParams journalParams = journalParamsOptional.get();
      JournalRecord journalRecord = JournalUtil.buildJournalRecordByEvent(eventPayload,
        journalParams.journalActionType, journalParams.journalEntityType, journalParams.journalActionStatus);

      ensureRecordTitleIfNeeded(journalRecord, eventPayload);
      journalService.save(JsonObject.mapFrom(journalRecord), tenantId);
    }
  }

  private void ensureRecordTitleIfNeeded(JournalRecord journalRecord, DataImportEventPayload eventPayload) {
    String recordAsString = eventPayload.getContext().get(ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC.value());

    if (journalRecord.getEntityType().equals(MARC_BIBLIOGRAPHIC) && StringUtils.isNotBlank(recordAsString)) {
      Record record = Json.decodeValue(recordAsString, Record.class);
      if (record.getParsedRecord() != null) {
        journalRecord.setTitle(ParsedRecordUtil.retrieveDataByField(record.getParsedRecord(), "245", titleSubfields));
      }
    }
  }
}
