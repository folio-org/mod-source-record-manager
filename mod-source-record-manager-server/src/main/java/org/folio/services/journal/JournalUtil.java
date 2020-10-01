package org.folio.services.journal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonObject;
import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.Record;

import java.util.Date;

import static org.apache.commons.lang3.StringUtils.isAnyEmpty;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;

/**
 * Journal util class for building specific 'JournalRecord'-objects, based on parameters.
 */
public class JournalUtil {

  private static final String EVENT_HAS_NO_DATA_MSG = "Failed to handle %s event, because event payload context does not contain %s and/or %s data";
  private static final String INSTANCE_OR_RECORD_MAPPING_EXCEPTION_MSG = "Can`t map 'record' or/and 'instance'";

  private JournalUtil() {

  }

  public static JournalRecord buildJournalRecordByEvent(DataImportEventPayload event, JournalRecord.ActionType actionType,
                                                        JournalRecord.EntityType entityType, JournalRecord.ActionStatus actionStatus) throws JournalRecordMapperException {
    String entityAsString = event.getContext().get(entityType.value());
    String recordAsString = event.getContext().get(MARC_BIBLIOGRAPHIC.value());
    if (isAnyEmpty(entityAsString, recordAsString)) {
      throw new JournalRecordMapperException(String.format(EVENT_HAS_NO_DATA_MSG, event.getEventType(),
        INSTANCE.value(), MARC_BIBLIOGRAPHIC.value()));
    }
    return buildJournalRecord(event, actionType, entityType, actionStatus, recordAsString);
  }

  public static JournalRecord buildJournalRecord(DataImportEventPayload eventPayload, JournalRecord.ActionType actionType, JournalRecord.EntityType entityType,
                                                 JournalRecord.ActionStatus actionStatus) throws JournalRecordMapperException {
    try {
      String recordAsString = eventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
      String entityAsString = eventPayload.getContext().get(entityType.value());
      Record record = new ObjectMapper().readValue(recordAsString, Record.class);
      JsonObject entityJson = new JsonObject(entityAsString);

      JournalRecord journalRecord = new JournalRecord()
        .withJobExecutionId(record.getSnapshotId())
        .withSourceId(record.getId())
        .withSourceRecordOrder(record.getOrder())
        .withEntityType(entityType)
        .withEntityId(entityJson.getString("id"))
        .withActionType(actionType)
        .withActionDate(new Date())
        .withActionStatus(actionStatus);

      if (entityType == INSTANCE || entityType == HOLDINGS || entityType == ITEM) {
        journalRecord.setEntityHrId(entityJson.getString("hrid"));
      }
      return journalRecord;
    } catch (Exception e) {
      throw new JournalRecordMapperException(INSTANCE_OR_RECORD_MAPPING_EXCEPTION_MSG, e);
    }
  }

  public static JournalRecord buildJournalRecord(DataImportEventPayload eventPayload, JournalRecord.ActionType actionType, JournalRecord.EntityType entityType,
                                                 JournalRecord.ActionStatus actionStatus, String errorMessage) throws JournalRecordMapperException {
    JournalRecord journalRecord = buildJournalRecord(eventPayload, actionType, entityType, actionStatus);
    return journalRecord.withError(errorMessage);
  }
}
