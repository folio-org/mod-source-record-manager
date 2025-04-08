package org.folio.services.journal;

import com.google.common.collect.Lists;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.eventbus.MessageProducer;
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
import java.util.Collection;
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
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.CREATE;
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
  public static final String MARC_BIB_RECORD_CREATED = "MARC_BIB_RECORD_CREATED";
  public static final String INCOMING_RECORD_ID = "INCOMING_RECORD_ID";
  public static final String BATCH_JOURNAL_ADDRESS = "batch-journal-queue";

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

  private static String extractRecord(Map<String, String> context) {
    return Optional.ofNullable(context.get(MARC_BIBLIOGRAPHIC.value()))
      .or(() -> Optional.ofNullable(context.get(MARC_AUTHORITY.value())))
      .or(() -> Optional.ofNullable(context.get(MARC_HOLDINGS.value())))
      .orElse(EMPTY);
  }

  public static List<JournalRecord> buildJournalRecordsByRecords(List<Record> records) {
    return records.stream().map(sourceRecord -> {
      var journalRecord = new JournalRecord()
        .withId(UUID.randomUUID().toString())
        .withJobExecutionId(sourceRecord.getSnapshotId())
        .withSourceId(sourceRecord.getId())
        .withSourceRecordOrder(sourceRecord.getOrder())
        .withActionType(JournalRecord.ActionType.PARSE)
        .withActionDate(new Date())
        .withActionStatus(sourceRecord.getErrorRecord() == null ? JournalRecord.ActionStatus.COMPLETED : JournalRecord.ActionStatus.ERROR);
      if (sourceRecord.getErrorRecord() != null) {
        journalRecord.setError(sourceRecord.getErrorRecord().getDescription());
      }
      return journalRecord;
    }).toList();
  }

  public static List<IncomingRecord> buildIncomingRecordsByRecords(List<Record> records) {
    return records.stream().map(sourceRecord -> {
      var incomingRecord = new IncomingRecord()
        .withId(sourceRecord.getId())
        .withJobExecutionId(sourceRecord.getSnapshotId())
        .withOrder(sourceRecord.getOrder())
        .withRawRecordContent(sourceRecord.getRawRecord().getContent());
      if (sourceRecord.getRecordType() != null) {
        incomingRecord.setRecordType(IncomingRecord.RecordType.fromValue(sourceRecord.getRecordType().value()));
      }
      if (sourceRecord.getParsedRecord() != null) {
        incomingRecord.setParsedRecordContent(String.valueOf(sourceRecord.getParsedRecord().getContent()));
      }
      return incomingRecord;
    }).toList();
  }

  public static List<JournalRecord> buildJournalRecordsByEvent(DataImportEventPayload eventPayload,
                                                               JournalRecord.ActionType actionType,
                                                               JournalRecord.EntityType entityType,
                                                               JournalRecord.ActionStatus actionStatus)
    throws JournalRecordMapperException {
    try {
      var context = eventPayload.getContext();
      var sourceRecord = getRecordFromContext(context, eventPayload);
      var incomingRecordId = getIncomingRecordId(context, sourceRecord);
      var entityJsonString = context.get(entityType.value());

      // Build the base record.
      var baseRecord = buildCommonJournalRecord(actionStatus, actionType, sourceRecord, eventPayload, context, incomingRecordId)
        .withEntityType(entityType);

      // [Change 1] For entityType INSTANCE: do not save logs if eventType is DI_SRS_MARC_AUTHORITY_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.
      if (entityType == INSTANCE &&
        DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING.value().equals(eventPayload.getEventType())) {
        return List.of();
      }

      // Preserve the original related entity branch for MATCH/NON_MATCH.
      if (isRelatedEntityRecordNeeded(entityType, actionType)) {
        var relatedRecord = buildCommonJournalRecord(actionStatus, actionType, sourceRecord, eventPayload, context, incomingRecordId)
          .withEntityType(ENTITY_TO_RELATED_ENTITY.get(entityType));
        return Lists.newArrayList(baseRecord, relatedRecord);
      }

      // Handle blank records for HOLDINGS or ITEM if needed.
      if (shouldConstructBlankRecords(actionType, entityType)) {
        return constructBlankRecords(context, sourceRecord, actionStatus, baseRecord, entityType, incomingRecordId, actionType);
      }

      // [Change 2] For entityType MARC_BIBLIOGRAPHIC:
      // If actionStatus == COMPLETED, build two records: one from the MARC_BIB JSON and one from the INSTANCE JSON.
      if (entityType == JournalRecord.EntityType.MARC_BIBLIOGRAPHIC) {
        if (!isEmpty(entityJsonString)) {
          var json = new JsonObject(entityJsonString);
          baseRecord.setEntityId(json.getString(MATCHED_ID_KEY));
        }
        if (actionStatus == JournalRecord.ActionStatus.COMPLETED) {
          String instanceJsonString = context.get(INSTANCE.value());
          JournalRecord instanceRecord;
          if (!isEmpty(instanceJsonString)) {
            var instanceJson = new JsonObject(instanceJsonString);
            instanceRecord = buildCommonJournalRecord(actionStatus, actionType, sourceRecord, eventPayload, context, incomingRecordId)
              .withEntityType(INSTANCE);
            // [Change 3] Populate instance record with hrid and id.
            instanceRecord.setEntityId(instanceJson.getString(ID_KEY));
            instanceRecord.setEntityHrId(instanceJson.getString(HRID_KEY));
          } else {
            instanceRecord = buildCommonJournalRecord(actionStatus, actionType, sourceRecord, eventPayload, context, incomingRecordId)
              .withEntityType(INSTANCE);
          }
          return Lists.newArrayList(baseRecord, instanceRecord);
        } else {
          return Lists.newArrayList(baseRecord);
        }
      }

      // For all other entity types:
      if (!isEmpty(entityJsonString)) {
        var ctx = new ProcessEntityContext(context, sourceRecord, eventPayload, incomingRecordId, baseRecord, entityJsonString);
        if (entityType == INSTANCE) {
          return processInstanceOrPoLineOrAuthority(ctx, INSTANCE, actionType, actionStatus);
        }
        return processEntity(ctx, entityType, actionType, actionStatus);
      } else if (isDiErrorEvent(eventPayload, context)) {
        var marcBibRecord = buildJournalRecordWithMarcBibType(actionStatus, actionType, sourceRecord, eventPayload, context, incomingRecordId);
        return Lists.newArrayList(baseRecord, marcBibRecord);
      }
      return Lists.newArrayList(baseRecord);
    } catch (Exception e) {
      LOGGER.warn("buildJournalRecordsByEvent:: Error while building JournalRecords, entityType: {}",
        entityType.value(), e);
      throw new JournalRecordMapperException(String.format(ENTITY_OR_RECORD_MAPPING_EXCEPTION_MSG, entityType.value()), e);
    }
  }

  private static List<JournalRecord> processEntity(ProcessEntityContext ctx,
                                                   JournalRecord.EntityType entityType,
                                                   JournalRecord.ActionType actionType,
                                                   JournalRecord.ActionStatus actionStatus) {
    if (isMarcEntity(entityType)) {
      var json = new JsonObject(ctx.entityJsonString());
      ctx.baseRecord().setEntityId(json.getString(MATCHED_ID_KEY));
      return Lists.newArrayList(ctx.baseRecord());
    }
    if (isInstanceOrPoLineOrAuthority(entityType)) {
      return processInstanceOrPoLineOrAuthority(ctx, entityType, actionType, actionStatus);
    }
    if (shouldProcessHoldingsItems(ctx, entityType)) {
      var result = new ArrayList<JournalRecord>();
      if (entityType == HOLDINGS) {
        result.addAll(processHoldings(actionType, entityType, actionStatus, ctx.context(), ctx.sourceRecord(), ctx.incomingRecordId()));
      }
      if (entityType == ITEM) {
        result.addAll(processItems(actionType, entityType, actionStatus, ctx.context(), ctx.sourceRecord(), ctx.incomingRecordId()));
      }
      if (ctx.context().get(MULTIPLE_ERRORS_KEY) != null) {
        result.addAll(processErrors(actionType, entityType, ctx.context(), ctx.sourceRecord(), ctx.incomingRecordId()));
      }
      return result;
    }
    return Lists.newArrayList(ctx.baseRecord());
  }

  private static List<JournalRecord> processInstanceOrPoLineOrAuthority(ProcessEntityContext ctx,
                                                                        JournalRecord.EntityType entityType,
                                                                        JournalRecord.ActionType actionType,
                                                                        JournalRecord.ActionStatus actionStatus) {
    var json = new JsonObject(ctx.entityJsonString());
    ctx.baseRecord().setEntityId(json.getString(ID_KEY));
    if (entityType == INSTANCE || entityType == PO_LINE) {
      ctx.baseRecord().setEntityHrId(json.getString(HRID_KEY));
    }
    if (entityType == PO_LINE) {
      ctx.baseRecord().setOrderId(json.getString("purchaseOrderId"));
    }
    if (entityType == INSTANCE && (actionType == UPDATE || actionType == CREATE)) {
      var marcBibRecord = buildJournalRecordWithMarcBibType(actionStatus, actionType,
        ctx.sourceRecord(), ctx.eventPayload(), ctx.context(), ctx.incomingRecordId());
      return Lists.newArrayList(ctx.baseRecord(), marcBibRecord);
    }
    return Lists.newArrayList(ctx.baseRecord());
  }

  private static Record getRecordFromContext(Map<String, String> context, DataImportEventPayload eventPayload) {
    var recordAsString = extractRecord(context);
    if (StringUtils.isBlank(recordAsString)) {
      return new Record()
        .withId(UUID.randomUUID().toString())
        .withSnapshotId(eventPayload.getJobExecutionId())
        .withOrder(0);
    }
    return Json.decodeValue(recordAsString, Record.class);
  }

  private static String getIncomingRecordId(Map<String, String> context, Record sourceRecord) {
    return context.get(INCOMING_RECORD_ID) != null ? context.get(INCOMING_RECORD_ID) : sourceRecord.getId();
  }

  private static boolean isRelatedEntityRecordNeeded(JournalRecord.EntityType entityType, JournalRecord.ActionType actionType) {
    return ENTITY_TO_RELATED_ENTITY.containsKey(entityType) &&
      (actionType == MATCH || actionType == NON_MATCH);
  }

  private static boolean shouldConstructBlankRecords(JournalRecord.ActionType actionType, JournalRecord.EntityType entityType) {
    return (actionType == MATCH || actionType == NON_MATCH) &&
      (entityType == HOLDINGS || entityType == ITEM);
  }

  private static List<JournalRecord> constructBlankRecords(Map<String, String> context,
                                                           Record sourceRecord,
                                                           JournalRecord.ActionStatus actionStatus,
                                                           JournalRecord baseRecord,
                                                           JournalRecord.EntityType entityType,
                                                           String incomingRecordId,
                                                           JournalRecord.ActionType actionType) {
    var records = new ArrayList<JournalRecord>();
    var notMatchedNumberField = context.get(NOT_MATCHED_NUMBER);
    var isNotMatchedNumberPresent = isNotBlank(notMatchedNumberField);
    if (isNotMatchedNumberPresent || actionType == NON_MATCH) {
      var notMatchedNumber = isNotMatchedNumberPresent ? Integer.parseInt(notMatchedNumberField) : 1;
      for (int i = 0; i < notMatchedNumber; i++) {
        records.add(constructBlankJournalRecord(sourceRecord, entityType, actionStatus, baseRecord.getError(), incomingRecordId)
          .withEntityType(entityType));
      }
    }
    return records;
  }

  private static boolean isDiErrorEvent(DataImportEventPayload eventPayload, Map<String, String> context) {
    return eventPayload.getEventType().equals(DI_ERROR.value()) &&
      context.containsKey(MARC_BIBLIOGRAPHIC.value());
  }

  private static boolean isMarcEntity(JournalRecord.EntityType entityType) {
    return entityType == MARC_BIBLIOGRAPHIC ||
      entityType == MARC_AUTHORITY ||
      entityType == MARC_HOLDINGS;
  }

  private static boolean isInstanceOrPoLineOrAuthority(JournalRecord.EntityType entityType) {
    return entityType == INSTANCE ||
      entityType == PO_LINE ||
      entityType == AUTHORITY;
  }

  private static boolean shouldProcessHoldingsItems(ProcessEntityContext ctx, JournalRecord.EntityType entityType) {
    return (entityType == HOLDINGS || entityType == ITEM || ctx.context().get(MULTIPLE_ERRORS_KEY) != null)
      && DI_ERROR != DataImportEventTypes.fromValue(ctx.eventPayload().getEventType());
  }

  public static MessageProducer<Collection<BatchableJournalRecord>> getJournalMessageProducer(Vertx eventBusVertx) {
    return eventBusVertx.eventBus().sender(BATCH_JOURNAL_ADDRESS,
      new DeliveryOptions()
        .setLocalOnly(true)
        .setCodecName(BatchableJournalRecordCodec.class.getSimpleName()));
  }

  public static MessageConsumer<Collection<BatchableJournalRecord>> getJournalMessageConsumer(Vertx eventBusVertx) {
    return eventBusVertx.eventBus().localConsumer(BATCH_JOURNAL_ADDRESS);
  }

  /**
   * Register needed message codecs for journal processing
   */
  public static Vertx registerCodecs(Vertx vertx) {
    vertx.eventBus().registerCodec(new BatchableJournalRecordCodec());
    return vertx;
  }

  private static JournalRecord buildJournalRecordWithMarcBibType(JournalRecord.ActionStatus actionStatus, JournalRecord.ActionType actionType, Record currentRecord,
                                                                 DataImportEventPayload eventPayload, Map<String, String> eventPayloadContext, String incomingRecordId) {
    var marcBibEntityAsString = eventPayloadContext.get(MARC_BIBLIOGRAPHIC.value());
    var marcBibEntityId = new JsonObject(marcBibEntityAsString).getString(MATCHED_ID_KEY);

    var actionTypeForMarcBib = actionType;
    if (eventPayloadContext.containsKey(MARC_BIB_RECORD_CREATED)) {
      var actionTypeFromContext = eventPayloadContext.get(MARC_BIB_RECORD_CREATED);
      actionTypeForMarcBib = actionTypeFromContext.equals(Boolean.TRUE.toString())
        ? JournalRecord.ActionType.CREATE
        : UPDATE;
    }

    return buildCommonJournalRecord(actionStatus, actionTypeForMarcBib, currentRecord, eventPayload, eventPayloadContext, incomingRecordId)
      .withEntityId(marcBibEntityId)
      .withEntityType(MARC_BIBLIOGRAPHIC);
  }

  private static JournalRecord buildCommonJournalRecord(JournalRecord.ActionStatus actionStatus, JournalRecord.ActionType actionType, Record currentRecord,
                                                        DataImportEventPayload eventPayload, Map<String, String> eventPayloadContext, String incomingRecordId) {
    var tenantId = eventPayload.getContext().get(CENTRAL_TENANT_ID_KEY);
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
                                                     JournalRecord.ActionStatus actionStatus, Map<String, String> eventPayloadContext, Record sourceRecord,
                                                     String incomingRecordId) {
    var multipleHoldings = getJsonArrayOfHoldings(eventPayloadContext.get(entityType.value()));
    var journalRecords = new ArrayList<JournalRecord>();

    for (int i = 0; i < multipleHoldings.size(); i++) {
      var holdingsAsJson = multipleHoldings.getJsonObject(i);
      var journalRecord = new JournalRecord()
        .withJobExecutionId(sourceRecord.getSnapshotId())
        .withSourceId(incomingRecordId)
        .withSourceRecordOrder(sourceRecord.getOrder())
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
                                                  JournalRecord.ActionStatus actionStatus, Map<String, String> eventPayloadContext, Record sourceRecord,
                                                  String incomingRecordId) {
    var multipleItems = new JsonArray(eventPayloadContext.get(entityType.value()));
    var journalRecords = new ArrayList<JournalRecord>();

    for (int i = 0; i < multipleItems.size(); i++) {
      var itemAsJson = multipleItems.getJsonObject(i);
      var journalRecord = new JournalRecord()
        .withJobExecutionId(sourceRecord.getSnapshotId())
        .withSourceId(incomingRecordId)
        .withSourceRecordOrder(sourceRecord.getOrder())
        .withEntityType(entityType)
        .withActionType(actionType)
        .withActionDate(new Date())
        .withActionStatus(actionStatus)
        .withEntityId(itemAsJson.getString(ID_KEY))
        .withEntityHrId(itemAsJson.getString(HRID_KEY));

      if (eventPayloadContext.containsKey(INSTANCE.value())) {
        var instanceJson = new JsonObject(eventPayloadContext.get(INSTANCE.value()));
        journalRecord.setInstanceId(instanceJson.getString(ID_KEY));
      } else if (eventPayloadContext.containsKey(HOLDINGS.value())) {
        var holdingsIdInstanceId = initalizeHoldingsIdInstanceIdMap(eventPayloadContext);
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
                                                   Map<String, String> eventPayloadContext, Record sourceRecord, String incomingRecordId) {
    var errors = new JsonArray(eventPayloadContext.get(MULTIPLE_ERRORS_KEY));
    var journalErrorRecords = new ArrayList<JournalRecord>();

    for (int i = 0; i < errors.size(); i++) {
      var errorAsJson = errors.getJsonObject(i);
      var journalRecord = new JournalRecord()
        .withJobExecutionId(sourceRecord.getSnapshotId())
        .withSourceId(incomingRecordId)
        .withSourceRecordOrder(sourceRecord.getOrder())
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

  private static Map<String, String> initalizeHoldingsIdInstanceIdMap(Map<String, String> eventPayloadContext) {
    var holdingsIdInstanceId = new HashMap<String, String>();
    var multipleHoldings = new JsonArray(eventPayloadContext.get(HOLDINGS.value()));
    for (int j = 0; j < multipleHoldings.size(); j++) {
      var holding = multipleHoldings.getJsonObject(j);
      holdingsIdInstanceId.put(holding.getString(ID_KEY), holding.getString(INSTANCE_ID_KEY));
    }
    return holdingsIdInstanceId;
  }

  private static JournalRecord constructBlankJournalRecord(Record sourceRecord, JournalRecord.EntityType entityType,
                                                           JournalRecord.ActionStatus actionStatus, String error, String incomingRecordId) {
    return new JournalRecord()
      .withJobExecutionId(sourceRecord.getSnapshotId())
      .withSourceId(incomingRecordId)
      .withSourceRecordOrder(sourceRecord.getOrder())
      .withEntityType(entityType)
      .withActionType(JournalRecord.ActionType.NON_MATCH)
      .withActionDate(new Date())
      .withActionStatus(actionStatus)
      .withError(error);
  }

  private record ProcessEntityContext(
    Map<String, String> context,
    Record sourceRecord,
    DataImportEventPayload eventPayload,
    String incomingRecordId,
    JournalRecord baseRecord,
    String entityJsonString
  ) {
  }
}
