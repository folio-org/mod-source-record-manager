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
import java.util.Map;
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
  public static final String ID_KEY = "id";
  public static final String HOLDINGS_RECORD_ID_KEY = "holdingsRecordId";
  public static final String INSTANCE_ID_KEY = "instanceId";
  public static final String HRID_KEY = "hrid";
  public static final String PERMANENT_LOCATION_ID_KEY = "permanentLocationId";

  private JournalUtil() {

  }

  private static String extractRecord(HashMap<String, String> context) {
    return Optional.ofNullable(context.get(MARC_BIBLIOGRAPHIC.value()))
      .or(() -> Optional.ofNullable(context.get(MARC_AUTHORITY.value())))
      .or(() -> Optional.ofNullable(context.get(MARC_HOLDINGS.value())))
      .orElse(EMPTY);
  }

  public static List<JournalRecord> buildJournalRecordsByEvent(DataImportEventPayload eventPayload, JournalRecord.ActionType actionType, JournalRecord.EntityType entityType,
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

      if (DI_ERROR == DataImportEventTypes.fromValue(eventPayload.getEventType())) {
        journalRecord.setError(eventPayloadContext.get(ERROR_KEY));
      }

      if (!isEmpty(entityAsString)) {
        if (entityType == INSTANCE || entityType == PO_LINE) {
          JsonObject entityJson = new JsonObject(entityAsString);
          journalRecord.setEntityId(entityJson.getString(ID_KEY));
          if (entityType == PO_LINE) {
            journalRecord.setOrderId(entityJson.getString("purchaseOrderId"));
          }
          journalRecord.setEntityHrId(entityJson.getString(HRID_KEY));
          return Lists.newArrayList(journalRecord);
        }
        if (entityType == HOLDINGS || entityType == ITEM || eventPayloadContext.get(MULTIPLE_ERRORS_KEY) != null) {
          List<JournalRecord> resultedJournalRecords = new ArrayList<>();
          if (entityType == HOLDINGS) {
            resultedJournalRecords.addAll(processHoldings(actionType, entityType, actionStatus, eventPayloadContext, record));
          }
          if (entityType == ITEM) {
            resultedJournalRecords.addAll(processItems(actionType, entityType, actionStatus, eventPayloadContext, record));
          }
          if (eventPayloadContext.get(MULTIPLE_ERRORS_KEY) != null) {
            resultedJournalRecords.addAll(processErrors(actionType, entityType, actionStatus, eventPayloadContext, record));
          }
          return resultedJournalRecords;
        } else {
          return Lists.newArrayList(journalRecord);
        }
      }
      return Lists.newArrayList(journalRecord);
    } catch (Exception e) {
      throw new JournalRecordMapperException(String.format(ENTITY_OR_RECORD_MAPPING_EXCEPTION_MSG, entityType.value()), e);
    }
  }

  private static List<JournalRecord> processHoldings(JournalRecord.ActionType actionType, JournalRecord.EntityType entityType, JournalRecord.ActionStatus actionStatus, HashMap<String, String> eventPayloadContext, Record record) {
    JsonArray multipleHoldingss = new JsonArray(eventPayloadContext.get(entityType.value()));
    List<JournalRecord> journalRecords = new ArrayList<>();

    for (int i = 0; i < multipleHoldingss.size(); i++) {
      JsonObject HoldingsAsJson = new JsonObject(multipleHoldingss.getString(i));
      JournalRecord journalRecord = new JournalRecord()
        .withJobExecutionId(record.getSnapshotId())
        .withSourceId(record.getId())
        .withSourceRecordOrder(record.getOrder())
        .withEntityType(entityType)
        .withActionType(actionType)
        .withActionDate(new Date())
        .withActionStatus(actionStatus)
        .withEntityId(HoldingsAsJson.getString(ID_KEY))
        .withEntityHrId(HoldingsAsJson.getString(HRID_KEY))
        .withInstanceId(HoldingsAsJson.getString(INSTANCE_ID_KEY))
        .withPermanentLocationId(HoldingsAsJson.getString(PERMANENT_LOCATION_ID_KEY));
      journalRecords.add(journalRecord);
    }
    return journalRecords;
  }

  private static List<JournalRecord> processItems(JournalRecord.ActionType actionType, JournalRecord.EntityType entityType, JournalRecord.ActionStatus actionStatus, HashMap<String, String> eventPayloadContext, Record record) {
    JsonArray multipleItems = new JsonArray(eventPayloadContext.get(entityType.value()));
    List<JournalRecord> journalRecords = new ArrayList<>();

    for (int i = 0; i < multipleItems.size(); i++) {
      JsonObject itemAsJson = new JsonObject(multipleItems.getString(i));
      JournalRecord journalRecord = new JournalRecord()
        .withJobExecutionId(record.getSnapshotId())
        .withSourceId(record.getId())
        .withSourceRecordOrder(record.getOrder())
        .withEntityType(entityType)
        .withActionType(actionType)
        .withActionDate(new Date())
        .withActionStatus(actionStatus)
        .withEntityId(itemAsJson.getString(ID_KEY))
        .withEntityHrId(itemAsJson.getString(HRID_KEY))
        .withInstanceId(itemAsJson.getString(INSTANCE_ID_KEY));

      if (eventPayloadContext.containsKey(INSTANCE.value())) {
        JsonObject instanceJson = new JsonObject(eventPayloadContext.get(INSTANCE.value()));
        journalRecord.setInstanceId(instanceJson.getString(ID_KEY));
      } else if (eventPayloadContext.containsKey(HOLDINGS.value())) {
        Map<String, String> holdingsIdInstanceId = initalizeHoldingsIdInstanceIdMap(eventPayloadContext);
        journalRecord.setInstanceId(holdingsIdInstanceId.get(itemAsJson.getString(HOLDINGS_RECORD_ID_KEY)));
      }
      journalRecord.setHoldingsId(itemAsJson.getString(HOLDINGS_RECORD_ID_KEY));
      journalRecords.add(journalRecord);
    }
    return journalRecords;
  }

  private static List<JournalRecord> processErrors(JournalRecord.ActionType actionType, JournalRecord.EntityType entityType, JournalRecord.ActionStatus actionStatus, HashMap<String, String> eventPayloadContext, Record record) {
    JsonArray errors = new JsonArray(eventPayloadContext.get(MULTIPLE_ERRORS_KEY));
    List<JournalRecord> journalErrorRecords = new ArrayList<>();

    for (int i = 0; i < errors.size(); i++) {
      JsonObject errorAsJson = new JsonObject(errors.getString(i));
      JournalRecord journalRecord = new JournalRecord()
        .withJobExecutionId(record.getSnapshotId())
        .withSourceId(record.getId())
        .withSourceRecordOrder(record.getOrder())
        .withEntityType(entityType)
        .withActionType(actionType)
        .withActionDate(new Date())
        .withActionStatus(actionStatus)
        .withError(errorAsJson.getString("error"))
        .withEntityId(errorAsJson.getString(ID_KEY));
      journalErrorRecords.add(journalRecord);
    }
    return journalErrorRecords;
  }

  private static Map<String, String> initalizeHoldingsIdInstanceIdMap(HashMap<String, String> eventPayloadContext) {
    Map<String, String> holdingsIdInstanceId = new HashMap<>();
    JsonArray multipleHoldings = new JsonArray(eventPayloadContext.get(HOLDINGS.value()));
    for (int j = 0; j < multipleHoldings.size(); j++) {
      JsonObject holding = new JsonObject(multipleHoldings.getString(j));
      holdingsIdInstanceId.put(holding.getString(ID_KEY), holding.getString(INSTANCE_ID_KEY));
    }
    return holdingsIdInstanceId;
  }
}
