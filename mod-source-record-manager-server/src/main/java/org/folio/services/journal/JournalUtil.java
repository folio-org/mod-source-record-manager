package org.folio.services.journal;

import com.google.common.collect.Lists;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.IncomingRecord;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.Record;

import java.util.ArrayList;
import java.util.Date;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.MATCH;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.NON_MATCH;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.UPDATE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.AUTHORITY;
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

  private static final Logger LOGGER = LogManager.getLogger();
  public static final String ERROR_KEY = "ERROR";
  private static final String ENTITY_OR_RECORD_MAPPING_EXCEPTION_MSG = "Can`t map 'RECORD' or/and '%s'";
  public static final String MULTIPLE_ERRORS_KEY = "ERRORS";
  public static final String ID_KEY = "id";
  public static final String HOLDING_ID_KEY = "holdingId";
  public static final String HOLDINGS_RECORD_ID_KEY = "holdingsRecordId";
  public static final String INSTANCE_ID_KEY = "instanceId";
  public static final String HRID_KEY = "hrid";
  public static final String MATCHED_ID_KEY = "matchedId";
  private static final String NOT_MATCHED_NUMBER = "NOT_MATCHED_NUMBER";
  public static final String PERMANENT_LOCATION_ID_KEY = "permanentLocationId";
  private static final String CENTRAL_TENANT_ID_KEY = "CENTRAL_TENANT_ID";
  private static final String CURRENT_EVENT_TYPE = "CURRENT_EVENT_TYPE";
  public static final String MARC_BIB_RECORD_CREATED = "MARC_BIB_RECORD_CREATED";
  public static final String INCOMING_RECORD_ID = "INCOMING_RECORD_ID";

  private static final EnumMap<JournalRecord.EntityType, JournalRecord.EntityType> ENTITY_TO_RELATED_ENTITY;

  static {
    ENTITY_TO_RELATED_ENTITY = new EnumMap<>(JournalRecord.EntityType.class);
    ENTITY_TO_RELATED_ENTITY.put(INSTANCE, MARC_BIBLIOGRAPHIC);
    ENTITY_TO_RELATED_ENTITY.put(MARC_BIBLIOGRAPHIC, INSTANCE);
    ENTITY_TO_RELATED_ENTITY.put(AUTHORITY, MARC_AUTHORITY);
    ENTITY_TO_RELATED_ENTITY.put(MARC_AUTHORITY, AUTHORITY);
  }

  private JournalUtil() {

  }

  private static String extractRecord(HashMap<String, String> context) {
    return Optional.ofNullable(context.get(MARC_BIBLIOGRAPHIC.value()))
      .or(() -> Optional.ofNullable(context.get(MARC_AUTHORITY.value())))
      .or(() -> Optional.ofNullable(context.get(MARC_HOLDINGS.value())))
      .orElse(EMPTY);
  }

  public static List<JournalRecord> buildJournalRecordsByRecords(List<Record> records) {
    return records.stream().map(record -> {
      JournalRecord journalRecord = new JournalRecord()
        .withId(UUID.randomUUID().toString())
        .withJobExecutionId(record.getSnapshotId())
        .withSourceId(record.getId())
        .withSourceRecordOrder(record.getOrder())
        .withActionType(JournalRecord.ActionType.PARSE)
        .withActionDate(new Date())
        .withActionStatus(record.getErrorRecord() == null ? JournalRecord.ActionStatus.COMPLETED : JournalRecord.ActionStatus.ERROR);
      if (record.getErrorRecord() != null) {
        journalRecord.setError(record.getErrorRecord().getDescription());
      }
      return journalRecord;
    }).toList();
  }

  public static List<IncomingRecord> buildIncomingRecordsByRecords(List<Record> records) {
    return records.stream().map(record -> {
      IncomingRecord incomingRecord = new IncomingRecord()
        .withId(record.getId())
        .withJobExecutionId(record.getSnapshotId())
        .withOrder(record.getOrder())
        .withRawRecordContent(record.getRawRecord().getContent());
      if (record.getRecordType() != null) {
        incomingRecord.setRecordType(IncomingRecord.RecordType.fromValue(record.getRecordType().value()));
      }
      if (record.getParsedRecord() != null) {
        incomingRecord.setParsedRecordContent(String.valueOf(record.getParsedRecord().getContent()));
      }
      return incomingRecord;
    }).toList();
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
        record = Json.decodeValue(recordAsString, Record.class);
      }
      String incomingRecordId = eventPayloadContext.get(INCOMING_RECORD_ID) != null ? eventPayloadContext.get(INCOMING_RECORD_ID) : record.getId();

      String entityAsString = eventPayloadContext.get(entityType.value());
      JournalRecord journalRecord = buildCommonJournalRecord(actionStatus, actionType, record, eventPayload, eventPayloadContext, incomingRecordId)
        .withEntityType(entityType);

      if (ENTITY_TO_RELATED_ENTITY.containsKey(entityType) && (actionType == MATCH || actionType == NON_MATCH)) {
        JournalRecord relatedEntityJournalRecord = buildCommonJournalRecord(actionStatus, actionType, record, eventPayload, eventPayloadContext, incomingRecordId)
          .withEntityType(ENTITY_TO_RELATED_ENTITY.get(entityType));

        return Lists.newArrayList(journalRecord, relatedEntityJournalRecord);
      }

      if ((actionType == JournalRecord.ActionType.MATCH || actionType == JournalRecord.ActionType.NON_MATCH)
        && (entityType == HOLDINGS || entityType == ITEM)) {
        List<JournalRecord> resultedJournalRecords = new ArrayList<>();

        String notMatchedNumberField = eventPayloadContext.get(NOT_MATCHED_NUMBER);
        boolean isNotMatchedNumberPresent = isNotBlank(notMatchedNumberField);

        if (isNotMatchedNumberPresent || actionType == JournalRecord.ActionType.NON_MATCH) {
          int notMatchedNumber = isNotMatchedNumberPresent ? Integer.parseInt(notMatchedNumberField) : 1;
          for (int i = 0; i < notMatchedNumber; i++) {
            resultedJournalRecords.add(constructBlankJournalRecord(record, entityType, actionStatus, journalRecord.getError(), incomingRecordId)
              .withEntityType(entityType));
          }
        }
        return resultedJournalRecords;
      }

      if (!isEmpty(entityAsString)) {
        if (entityType == MARC_BIBLIOGRAPHIC || entityType == MARC_AUTHORITY || entityType == MARC_HOLDINGS) {
          var entityId = new JsonObject(entityAsString).getString(MATCHED_ID_KEY);
          journalRecord.setEntityId(entityId);
          return Lists.newArrayList(journalRecord);
        }
        if (entityType == INSTANCE || entityType == PO_LINE || entityType == AUTHORITY) {
          JsonObject entityJson = new JsonObject(entityAsString);
          journalRecord.setEntityId(entityJson.getString(ID_KEY));
          if (entityType == INSTANCE || entityType == PO_LINE) {
            journalRecord.setEntityHrId(entityJson.getString(HRID_KEY));
          }
          if (entityType == PO_LINE) {
            journalRecord.setOrderId(entityJson.getString("purchaseOrderId"));
          }
          if (eventPayload.getEventType().equals(DI_INVENTORY_INSTANCE_CREATED.value()) ||
            isCreateOrUpdateInstanceEventReceived(eventPayloadContext)) {
            var journalRecordWithMarcBib = buildJournalRecordWithMarcBibType(actionStatus, actionType, record, eventPayload, eventPayloadContext, incomingRecordId);
            return Lists.newArrayList(journalRecord, journalRecordWithMarcBib);
          }
          return Lists.newArrayList(journalRecord);
        }
        if ((entityType == HOLDINGS || entityType == ITEM || eventPayloadContext.get(MULTIPLE_ERRORS_KEY) != null)
          && DI_ERROR != DataImportEventTypes.fromValue(eventPayload.getEventType())) {
          List<JournalRecord> resultedJournalRecords = new ArrayList<>();
          if (entityType == HOLDINGS) {
            resultedJournalRecords.addAll(processHoldings(actionType, entityType, actionStatus, eventPayloadContext, record, incomingRecordId));
          }
          if (entityType == ITEM) {
            resultedJournalRecords.addAll(processItems(actionType, entityType, actionStatus, eventPayloadContext, record, incomingRecordId));
          }
          if (eventPayloadContext.get(MULTIPLE_ERRORS_KEY) != null) {
            resultedJournalRecords.addAll(processErrors(actionType, entityType, eventPayloadContext, record, incomingRecordId));
          }
          return resultedJournalRecords;
        } else {
          return Lists.newArrayList(journalRecord);
        }
      } else {
        if (eventPayload.getEventType().equals(DI_ERROR.value()) && eventPayloadContext.containsKey(MARC_BIBLIOGRAPHIC.value())) {
          var journalRecordWithMarcBib = buildJournalRecordWithMarcBibType(actionStatus, actionType, record, eventPayload, eventPayloadContext, incomingRecordId);
          return Lists.newArrayList(journalRecord, journalRecordWithMarcBib);
        }
      }
      return Lists.newArrayList(journalRecord);
    } catch (Exception e) {
      LOGGER.warn("buildJournalRecordsByEvent:: Error while build JournalRecords, entityType: {}", entityType.value(), e);
      throw new JournalRecordMapperException(String.format(ENTITY_OR_RECORD_MAPPING_EXCEPTION_MSG, entityType.value()), e);
    }
  }

  private static boolean isCreateOrUpdateInstanceEventReceived(HashMap<String, String> eventPayloadContext) {
    if (eventPayloadContext.containsKey(CURRENT_EVENT_TYPE)) {
      var currentEventType = DataImportEventTypes.fromValue(eventPayloadContext.get(CURRENT_EVENT_TYPE));
      return ((DI_INVENTORY_INSTANCE_CREATED == currentEventType) || (DI_INVENTORY_INSTANCE_UPDATED == currentEventType));
    }
    return false;
  }

  private static JournalRecord buildJournalRecordWithMarcBibType(JournalRecord.ActionStatus actionStatus, JournalRecord.ActionType actionType, Record currentRecord,
                                                                 DataImportEventPayload eventPayload, HashMap<String, String> eventPayloadContext, String incomingRecordId) {
    String marcBibEntityAsString = eventPayloadContext.get(MARC_BIBLIOGRAPHIC.value());
    String marcBibEntityId = new JsonObject(marcBibEntityAsString).getString(MATCHED_ID_KEY);

    var actionTypeForMarcBib = actionType;
    if (eventPayloadContext.containsKey(MARC_BIB_RECORD_CREATED)) {
      String actionTypeFromContext = eventPayloadContext.get(MARC_BIB_RECORD_CREATED);

      if (actionTypeFromContext.equals(Boolean.TRUE.toString())) actionTypeForMarcBib = JournalRecord.ActionType.CREATE;
      else actionTypeForMarcBib = UPDATE;
    }

    return buildCommonJournalRecord(actionStatus, actionTypeForMarcBib, currentRecord, eventPayload, eventPayloadContext, incomingRecordId)
      .withEntityId(marcBibEntityId)
      .withEntityType(MARC_BIBLIOGRAPHIC);
  }

  private static JournalRecord buildCommonJournalRecord(JournalRecord.ActionStatus actionStatus, JournalRecord.ActionType actionType, Record currentRecord,
                                                        DataImportEventPayload eventPayload, HashMap<String, String> eventPayloadContext, String incomingRecordId) {
    String tenantId = eventPayload.getContext().get(CENTRAL_TENANT_ID_KEY);

    var currentJournalRecord = new JournalRecord()
      .withJobExecutionId(currentRecord.getSnapshotId())
      .withSourceId(incomingRecordId)
      .withSourceRecordOrder(currentRecord.getOrder())
      .withActionType(actionType)
      .withActionDate(new Date())
      .withActionStatus(actionStatus)
      // tenantId field is filled in only for the case when record/entity has been changed on central tenant
      // by data import initiated on a member tenant
      .withTenantId(tenantId);

    if (DI_ERROR == DataImportEventTypes.fromValue(eventPayload.getEventType())) {
      currentJournalRecord.setError(eventPayloadContext.get(ERROR_KEY));
    }

    return currentJournalRecord;
  }

  private static List<JournalRecord> processHoldings(JournalRecord.ActionType actionType, JournalRecord.EntityType entityType,
                                                     JournalRecord.ActionStatus actionStatus, HashMap<String, String> eventPayloadContext, Record record,
                                                     String incomingRecordId) {
    JsonArray multipleHoldings = getJsonArrayOfHoldings(eventPayloadContext.get(entityType.value()));
    List<JournalRecord> journalRecords = new ArrayList<>();

    for (int i = 0; i < multipleHoldings.size(); i++) {
      JsonObject holdingsAsJson = multipleHoldings.getJsonObject(i);
      JournalRecord journalRecord = new JournalRecord()
        .withJobExecutionId(record.getSnapshotId())
        .withSourceId(incomingRecordId)
        .withSourceRecordOrder(record.getOrder())
        .withEntityType(entityType)
        .withActionType(actionType)
        .withActionDate(new Date())
        .withActionStatus(actionStatus)
        .withEntityId(holdingsAsJson.getString(ID_KEY))
        .withEntityHrId(holdingsAsJson.getString(HRID_KEY))
        .withInstanceId(holdingsAsJson.getString(INSTANCE_ID_KEY))
        .withPermanentLocationId(holdingsAsJson.getString(PERMANENT_LOCATION_ID_KEY));
      journalRecords.add(journalRecord);
    }
    return journalRecords;
  }

  private static JsonArray getJsonArrayOfHoldings(String json) {
    try {
      return new JsonArray(json);
    } catch (Exception e) {
      return JsonArray.of(new JsonObject(json));
    }
  }

  private static List<JournalRecord> processItems(JournalRecord.ActionType actionType, JournalRecord.EntityType entityType,
                                                  JournalRecord.ActionStatus actionStatus, HashMap<String, String> eventPayloadContext, Record record,
                                                  String incomingRecordId) {
    JsonArray multipleItems = new JsonArray(eventPayloadContext.get(entityType.value()));
    List<JournalRecord> journalRecords = new ArrayList<>();

    for (int i = 0; i < multipleItems.size(); i++) {
      JsonObject itemAsJson = multipleItems.getJsonObject(i);
      JournalRecord journalRecord = new JournalRecord()
        .withJobExecutionId(record.getSnapshotId())
        .withSourceId(incomingRecordId)
        .withSourceRecordOrder(record.getOrder())
        .withEntityType(entityType)
        .withActionType(actionType)
        .withActionDate(new Date())
        .withActionStatus(actionStatus)
        .withEntityId(itemAsJson.getString(ID_KEY))
        .withEntityHrId(itemAsJson.getString(HRID_KEY));

      if (eventPayloadContext.containsKey(INSTANCE.value())) {
        JsonObject instanceJson = new JsonObject(eventPayloadContext.get(INSTANCE.value()));
        journalRecord.setInstanceId(instanceJson.getString(ID_KEY));
      } else if (eventPayloadContext.containsKey(HOLDINGS.value())) {
        Map<String, String> holdingsIdInstanceId = initalizeHoldingsIdInstanceIdMap(eventPayloadContext);
        journalRecord.setInstanceId(holdingsIdInstanceId.get(getHoldingsId(itemAsJson)));
      }
      journalRecord.setHoldingsId(getHoldingsId(itemAsJson));
      journalRecords.add(journalRecord);
    }
    return journalRecords;
  }

  private static String getHoldingsId(JsonObject jsonObject) {
    return jsonObject.getString(HOLDINGS_RECORD_ID_KEY) != null
      ? jsonObject.getString(HOLDINGS_RECORD_ID_KEY)
      : jsonObject.getString(HOLDING_ID_KEY);
  }

  private static List<JournalRecord> processErrors(JournalRecord.ActionType actionType, JournalRecord.EntityType entityType,
                                                   HashMap<String, String> eventPayloadContext, Record record, String incomingRecordId) {
    JsonArray errors = new JsonArray(eventPayloadContext.get(MULTIPLE_ERRORS_KEY));
    List<JournalRecord> journalErrorRecords = new ArrayList<>();

    for (int i = 0; i < errors.size(); i++) {
      JsonObject errorAsJson = errors.getJsonObject(i);
      JournalRecord journalRecord = new JournalRecord()
        .withJobExecutionId(record.getSnapshotId())
        .withSourceId(incomingRecordId)
        .withSourceRecordOrder(record.getOrder())
        .withEntityType(entityType)
        .withActionType(actionType)
        .withActionDate(new Date())
        .withActionStatus(JournalRecord.ActionStatus.ERROR)
        .withError(errorAsJson.getString("error"))
        .withEntityId(errorAsJson.getString(ID_KEY));
      if (entityType == ITEM) {
        journalRecord.withHoldingsId(errorAsJson.getString(HOLDING_ID_KEY));
      }
      journalErrorRecords.add(journalRecord);
    }
    return journalErrorRecords;
  }

  private static Map<String, String> initalizeHoldingsIdInstanceIdMap(HashMap<String, String> eventPayloadContext) {
    Map<String, String> holdingsIdInstanceId = new HashMap<>();
    JsonArray multipleHoldings = new JsonArray(eventPayloadContext.get(HOLDINGS.value()));
    for (int j = 0; j < multipleHoldings.size(); j++) {
      JsonObject holding = multipleHoldings.getJsonObject(j);
      holdingsIdInstanceId.put(holding.getString(ID_KEY), holding.getString(INSTANCE_ID_KEY));
    }
    return holdingsIdInstanceId;
  }

  private static JournalRecord constructBlankJournalRecord(Record record, JournalRecord.EntityType entityType,
                                                           JournalRecord.ActionStatus actionStatus, String error, String incomingRecordId) {
    return new JournalRecord()
      .withJobExecutionId(record.getSnapshotId())
      .withSourceId(incomingRecordId)
      .withSourceRecordOrder(record.getOrder())
      .withEntityType(entityType)
      .withActionType(JournalRecord.ActionType.NON_MATCH)
      .withActionDate(new Date())
      .withActionStatus(actionStatus)
      .withError(error);
  }
}
