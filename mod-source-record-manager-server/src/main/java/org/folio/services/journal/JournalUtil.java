package org.folio.services.journal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.Record;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_HOLDINGS;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.PO_LINE;

/**
 * Journal util class for building specific 'JournalRecord'-objects, based on parameters.
 */
public class JournalUtil {

  public static final String ERROR_KEY = "ERROR";
  private static final String ENTITY_OR_RECORD_MAPPING_EXCEPTION_MSG = "Can`t map 'RECORD' or/and '%s'";
  public static final String MULTIPLE_ERRORS_KEY = "ERRORS";

  private JournalUtil() {

  }

  private static String extractRecord(HashMap<String, String> context) {
    return Optional.ofNullable(context.get(MARC_BIBLIOGRAPHIC.value()))
      .or(() -> Optional.ofNullable(context.get(MARC_AUTHORITY.value())))
      .or(() -> Optional.ofNullable(context.get(MARC_HOLDINGS.value())))
      .orElse(EMPTY);
  }

  public static List<JournalRecord> buildJournalRecordByEvent(DataImportEventPayload eventPayload, JournalRecord.ActionType actionType, JournalRecord.EntityType entityType,
                                                              JournalRecord.ActionStatus actionStatus) throws JournalRecordMapperException {
    try {
      HashMap<String, String> eventPayloadContext = eventPayload.getContext();

      String recordAsString = extractRecord(eventPayloadContext);
      Record record;
      if (StringUtils.isBlank(recordAsString)) {
        // create stub record since none was introduced
        record = new Record()
          .withId(UUID.randomUUID().toString())
          .withSnapshotId(eventPayload.getJobExecutionId())
          .withOrder(0);
      } else {
        record = new ObjectMapper().readValue(recordAsString, Record.class);
      }

      String entityAsString = eventPayloadContext.get(entityType.value());
      JournalRecord journalRecord = new JournalRecord()
        .withJobExecutionId(record.getSnapshotId())
        .withSourceId(record.getId())
        .withSourceRecordOrder(record.getOrder())
        .withEntityType(entityType)
        .withActionType(actionType)
        .withActionDate(new Date())
        .withActionStatus(actionStatus);

      if (!isEmpty(entityAsString)) {
        JsonObject entityJson = new JsonObject(entityAsString);
        journalRecord.setEntityId(entityJson.getString("id"));
        if (entityType == INSTANCE || entityType == PO_LINE) {
          if (entityType == PO_LINE) {
            journalRecord.setOrderId(entityJson.getString("purchaseOrderId"));
          }
          journalRecord.setEntityHrId(entityJson.getString("hrid"));
          return Lists.newArrayList(journalRecord);
        }
        if (DI_ERROR == DataImportEventTypes.fromValue(eventPayload.getEventType())) {
          journalRecord.setError(eventPayloadContext.get(ERROR_KEY));
          return Lists.newArrayList(journalRecord);
        }
        if (entityType == HOLDINGS) {
          return processHoldings(actionType, entityType, actionStatus, eventPayloadContext, record);
        }
        if (entityType == ITEM) {
          return processItems(actionType, entityType, actionStatus, eventPayloadContext, record);
        }
        if (eventPayloadContext.get(MULTIPLE_ERRORS_KEY) != null) {
          return processErrors(actionType, entityType, actionStatus, eventPayloadContext, record);
        }
      }
      return Lists.newArrayList(journalRecord);
    } catch (Exception e) {
      throw new JournalRecordMapperException(String.format(ENTITY_OR_RECORD_MAPPING_EXCEPTION_MSG, entityType.value()), e);
    }
  }

  private static List<JournalRecord> processErrors(JournalRecord.ActionType actionType, JournalRecord.EntityType entityType, JournalRecord.ActionStatus actionStatus, HashMap<String, String> eventPayloadContext, Record record) {
    JsonArray errors = new JsonArray(eventPayloadContext.get(entityType.value()));
    List<JournalRecord> journalErrorRecords = new ArrayList<>();

    for (int i = 0; i < errors.size(); i++) {
      JsonObject errorAsJson = errors.getJsonObject(i);
      JournalRecord journalRecord = new JournalRecord()
        .withJobExecutionId(record.getSnapshotId())
        .withSourceId(record.getId())
        .withSourceRecordOrder(record.getOrder())
        .withEntityType(entityType)
        .withActionType(actionType)
        .withActionDate(new Date())
        .withActionStatus(actionStatus)
        .withError(errorAsJson.getString("error"))
        .withEntityId(errorAsJson.getString("id"));
      journalErrorRecords.add(journalRecord);
    }
    return journalErrorRecords;
  }

  private static List<JournalRecord> processHoldings(JournalRecord.ActionType actionType, JournalRecord.EntityType entityType, JournalRecord.ActionStatus actionStatus, HashMap<String, String> eventPayloadContext, Record record) {
    JsonArray multipleEntities = new JsonArray(eventPayloadContext.get(entityType.value()));
    List<JournalRecord> journalRecords = new ArrayList<>();

    for (int i = 0; i < multipleEntities.size(); i++) {
      JsonObject entityAsJson = multipleEntities.getJsonObject(i);
      JournalRecord journalRecord = new JournalRecord()
        .withJobExecutionId(record.getSnapshotId())
        .withSourceId(record.getId())
        .withSourceRecordOrder(record.getOrder())
        .withEntityType(entityType)
        .withActionType(actionType)
        .withActionDate(new Date())
        .withActionStatus(actionStatus)
        .withEntityId(entityAsJson.getString("id"))
        .withEntityHrId(entityAsJson.getString("hrid"))
        .withInstanceId(entityAsJson.getString("instanceId"))
        .withPermanentLocationId(entityAsJson.getString("permanentLocationId"));
      journalRecords.add(journalRecord);
    }
    return journalRecords;
  }

  private static List<JournalRecord> processItems(JournalRecord.ActionType actionType, JournalRecord.EntityType entityType, JournalRecord.ActionStatus actionStatus, HashMap<String, String> eventPayloadContext, Record record) {
    JsonArray multipleEntities = new JsonArray(eventPayloadContext.get(entityType.value()));
    List<JournalRecord> journalRecords = new ArrayList<>();

    for (int i = 0; i < multipleEntities.size(); i++) {
      JsonObject entityAsJson = multipleEntities.getJsonObject(i);
      JournalRecord journalRecord = new JournalRecord()
        .withJobExecutionId(record.getSnapshotId())
        .withSourceId(record.getId())
        .withSourceRecordOrder(record.getOrder())
        .withEntityType(entityType)
        .withActionType(actionType)
        .withActionDate(new Date())
        .withActionStatus(actionStatus)
        .withEntityId(entityAsJson.getString("id"))
        .withEntityHrId(entityAsJson.getString("hrid"))
        .withInstanceId(entityAsJson.getString("instanceId"));

      if (eventPayloadContext.containsKey(INSTANCE.value())) {
        JsonObject instanceJson = new JsonObject(eventPayloadContext.get(INSTANCE.value()));
        journalRecord.setInstanceId(instanceJson.getString("id"));
      } else if (eventPayloadContext.containsKey(HOLDINGS.value())) {
        JsonObject holdingsJson = new JsonObject(eventPayloadContext.get(HOLDINGS.value()));
        journalRecord.setInstanceId(holdingsJson.getString("instanceId"));
      }
      journalRecord.setHoldingsId(entityAsJson.getString("holdingsRecordId"));
      journalRecords.add(journalRecord);
    }
    return journalRecords;
  }

}
