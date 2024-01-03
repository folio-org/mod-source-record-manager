package org.folio.services.journal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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

import static org.apache.commons.lang.StringUtils.isNotBlank;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_LOG_SRS_MARC_BIB_RECORD_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_UPDATED;
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
  private static final String NOT_MATCHED_NUMBER = "NOT_MATCHED_NUMBER";
  public static final String PERMANENT_LOCATION_ID_KEY = "permanentLocationId";
  private static final String CENTRAL_TENANT_ID_KEY = "CENTRAL_TENANT_ID";

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
        .withActionStatus(actionStatus)
        // tenantId field is filled in only for the case when record/entity has been changed on central tenant
        // by data import initiated on a member tenant
        .withTenantId(eventPayload.getContext().get(CENTRAL_TENANT_ID_KEY));

      if (DI_ERROR == DataImportEventTypes.fromValue(eventPayload.getEventType())) {
        journalRecord.setError(eventPayloadContext.get(ERROR_KEY));
      }

      if ((actionType == JournalRecord.ActionType.MATCH || actionType == JournalRecord.ActionType.NON_MATCH)
        && (entityType == HOLDINGS || entityType == ITEM)) {
        List<JournalRecord> resultedJournalRecords = new ArrayList<>();

        String notMatchedNumberField = eventPayloadContext.get(NOT_MATCHED_NUMBER);
        boolean isNotMatchedNumberPresent = isNotBlank(notMatchedNumberField);

        if (isNotMatchedNumberPresent || actionType == JournalRecord.ActionType.NON_MATCH) {
          int notMatchedNumber = isNotMatchedNumberPresent ? Integer.parseInt(notMatchedNumberField) : 1;
          for (int i = 0; i < notMatchedNumber; i++) {
            resultedJournalRecords.add(constructBlankJournalRecord(record, entityType, actionStatus, journalRecord.getError())
              .withEntityType(entityType));
          }
        }
        return resultedJournalRecords;
      }

      if (!isEmpty(entityAsString)) {
        if (entityType == INSTANCE || entityType == PO_LINE || entityType == AUTHORITY ||
          (entityType == MARC_BIBLIOGRAPHIC && isMarcBibUpdateEventReceived(eventPayload))) {
          JsonObject entityJson = new JsonObject(entityAsString);
          journalRecord.setEntityId(entityJson.getString(ID_KEY));
          if (entityType == INSTANCE || entityType == PO_LINE) {
            journalRecord.setEntityHrId(entityJson.getString(HRID_KEY));
          }
          if (entityType == PO_LINE) {
            journalRecord.setOrderId(entityJson.getString("purchaseOrderId"));
          }
          return Lists.newArrayList(journalRecord);
        }
        if ((entityType == HOLDINGS || entityType == ITEM || eventPayloadContext.get(MULTIPLE_ERRORS_KEY) != null)
          && DI_ERROR != DataImportEventTypes.fromValue(eventPayload.getEventType())) {
          List<JournalRecord> resultedJournalRecords = new ArrayList<>();
          if (entityType == HOLDINGS) {
            resultedJournalRecords.addAll(processHoldings(actionType, entityType, actionStatus, eventPayloadContext, record));
          }
          if (entityType == ITEM) {
            resultedJournalRecords.addAll(processItems(actionType, entityType, actionStatus, eventPayloadContext, record));
          }
          if (eventPayloadContext.get(MULTIPLE_ERRORS_KEY) != null) {
            resultedJournalRecords.addAll(processErrors(actionType, entityType, eventPayloadContext, record));
          }
          return resultedJournalRecords;
        } else {
          return Lists.newArrayList(journalRecord);
        }
      }
      return Lists.newArrayList(journalRecord);
    } catch (Exception e) {
      LOGGER.warn("buildJournalRecordsByEvent:: Error while build JournalRecords, entityType: {}", entityType.value(), e);
      throw new JournalRecordMapperException(String.format(ENTITY_OR_RECORD_MAPPING_EXCEPTION_MSG, entityType.value()), e);
    }
  }

  private static boolean isMarcBibUpdateEventReceived(DataImportEventPayload eventPayload) {
    if (DI_LOG_SRS_MARC_BIB_RECORD_UPDATED == DataImportEventTypes.fromValue(eventPayload.getEventType())
      || DI_SRS_MARC_BIB_RECORD_UPDATED == DataImportEventTypes.fromValue(eventPayload.getEventType())) {
      return true;
    } else {
      Optional<String> optionalTheLastEvent = eventPayload.getEventsChain().stream().reduce((first, second) -> second);
      return optionalTheLastEvent.isPresent() && DI_SRS_MARC_BIB_RECORD_UPDATED == DataImportEventTypes.fromValue(optionalTheLastEvent.get());
    }
  }

  private static List<JournalRecord> processHoldings(JournalRecord.ActionType actionType, JournalRecord.EntityType entityType,
                                                     JournalRecord.ActionStatus actionStatus, HashMap<String, String> eventPayloadContext, Record record) {
    JsonArray multipleHoldings = getJsonArrayOfHoldings(eventPayloadContext.get(entityType.value()));
    List<JournalRecord> journalRecords = new ArrayList<>();

    for (int i = 0; i < multipleHoldings.size(); i++) {
      JsonObject holdingsAsJson = multipleHoldings.getJsonObject(i);
      JournalRecord journalRecord = new JournalRecord()
        .withJobExecutionId(record.getSnapshotId())
        .withSourceId(record.getId())
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
                                                  JournalRecord.ActionStatus actionStatus, HashMap<String, String> eventPayloadContext, Record record) {
    JsonArray multipleItems = new JsonArray(eventPayloadContext.get(entityType.value()));
    List<JournalRecord> journalRecords = new ArrayList<>();

    for (int i = 0; i < multipleItems.size(); i++) {
      JsonObject itemAsJson = multipleItems.getJsonObject(i);
      JournalRecord journalRecord = new JournalRecord()
        .withJobExecutionId(record.getSnapshotId())
        .withSourceId(record.getId())
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
                                                   HashMap<String, String> eventPayloadContext, Record record) {
    JsonArray errors = new JsonArray(eventPayloadContext.get(MULTIPLE_ERRORS_KEY));
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
                                                           JournalRecord.ActionStatus actionStatus, String error) {
    return new JournalRecord()
      .withJobExecutionId(record.getSnapshotId())
      .withSourceId(record.getId())
      .withSourceRecordOrder(record.getOrder())
      .withEntityType(entityType)
      .withActionType(JournalRecord.ActionType.NON_MATCH)
      .withActionDate(new Date())
      .withActionStatus(actionStatus)
      .withError(error);
  }
}
