package org.folio.services.journal;

import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.tools.utils.ObjectMapperTool;

/**
 * Journal util class for building specific 'JournalRecord'-objects, based on parameters.
 */
public class JournalUtil {

  private static final String EVENT_HAS_NO_DATA_MSG = "Failed to handle %s event, because event payload context does not contain %s and/or %s data";
  private static final String INSTANCE_OR_RECORD_MAPPING_EXCEPTION_MSG = "Can`t map 'record' or/and 'instance'";

  private JournalUtil(){

  }

  public static JournalRecord buildJournalRecordByEvent(DataImportEventPayload event, JournalRecord.ActionType actionType,
                                                        JournalRecord.EntityType entityType, JournalRecord.ActionStatus actionStatus) throws JournalRecordMapperException {
    String instanceAsString = event.getContext().get(INSTANCE.value());
    String recordAsString = event.getContext().get(MARC_BIBLIOGRAPHIC.value());
    if (StringUtils.isEmpty(instanceAsString) || StringUtils.isEmpty(recordAsString)) {
      throw new JournalRecordMapperException(String.format(EVENT_HAS_NO_DATA_MSG, event.getEventType(),
        INSTANCE.value(), MARC_BIBLIOGRAPHIC.value()));
    }
    return buildJournalRecord(actionType, entityType, actionStatus, instanceAsString, recordAsString);
  }

  private static JournalRecord buildJournalRecord(JournalRecord.ActionType actionType, JournalRecord.EntityType entityType,
                                                  JournalRecord.ActionStatus actionStatus, String instanceAsString, String recordAsString) throws JournalRecordMapperException {
    try {
      Record record = ObjectMapperTool.getMapper().readValue(recordAsString, Record.class);
      Instance instance = ObjectMapperTool.getMapper().readValue(instanceAsString, Instance.class);
      return new JournalRecord()
        .withJobExecutionId(record.getSnapshotId())
        .withSourceId(record.getId())
        .withSourceRecordOrder(record.getOrder())
        .withEntityType(entityType)
        .withEntityId(instance.getId())
        .withEntityHrId(instance.getHrid())
        .withActionType(actionType)
        .withActionDate(new Date())
        .withActionStatus(actionStatus);
    } catch (IOException e) {
      throw new JournalRecordMapperException(INSTANCE_OR_RECORD_MAPPING_EXCEPTION_MSG, e);
    }
  }
}
