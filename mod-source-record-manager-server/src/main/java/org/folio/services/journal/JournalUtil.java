package org.folio.services.journal;

import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.rest.impl.ChangeManagerHandlersImpl;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.tools.utils.ObjectMapperTool;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class JournalUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeManagerHandlersImpl.class);
  private static final String EVENT_HAS_NO_DATA_MSG = "Failed to handle CREATED_INVENTORY_INSTANCE event, cause event payload context does not contain INSTANCE and/or MARC_BIBLIOGRAPHIC data";

  private JournalUtil(){

  }

  public static JournalRecord buildJournalRecordByEvent(DataImportEventPayload event, JournalRecord.ActionType actionType) {
    String instanceAsString = event.getContext().get(INSTANCE.value());
    String recordAsString = event.getContext().get(MARC_BIBLIOGRAPHIC.value());

    if (StringUtils.isEmpty(instanceAsString) || StringUtils.isEmpty(recordAsString)) {
      LOGGER.error(EVENT_HAS_NO_DATA_MSG);
      return null; // TODO
    }

    try {
      Record record = ObjectMapperTool.getMapper().readValue(recordAsString, Record.class);
      Instance instance = ObjectMapperTool.getMapper().readValue(instanceAsString, Instance.class);

      return new JournalRecord()
        .withJobExecutionId(record.getSnapshotId())
        .withSourceId(record.getId())
        .withSourceRecordOrder(record.getOrder())
        .withEntityType(JournalRecord.EntityType.INSTANCE)
        .withEntityId(instance.getId())
        .withEntityHrId(instance.getHrid())
        .withActionType(actionType)
        .withActionDate(new Date())
        .withActionStatus(JournalRecord.ActionStatus.COMPLETED);


    } catch (IOException e ) {
      LOGGER.error("Can`t map record or/and instance", e);
    }
    return null; //TODO
  }
}
