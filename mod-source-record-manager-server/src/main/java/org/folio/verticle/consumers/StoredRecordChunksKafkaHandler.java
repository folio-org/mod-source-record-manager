package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.pgclient.PgException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.exception.ConflictException;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.JournalRecord.EntityType;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.RecordType;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.services.EventProcessedService;
import org.folio.services.MappingRuleCache;
import org.folio.services.RecordsPublishingService;
import org.folio.services.entity.MappingRuleCacheKey;
import org.folio.services.journal.JournalService;
import org.folio.services.util.ParsedRecordUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.commons.lang3.ObjectUtils.allNotNull;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_EDIFACT_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDING_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.COMPLETED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.CREATE;
import static org.folio.rest.jaxrs.model.Record.RecordType.EDIFACT;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_HOLDING;
import static org.folio.services.AbstractChunkProcessingService.UNIQUE_CONSTRAINT_VIOLATION_CODE;

@Component
@Qualifier("StoredRecordChunksKafkaHandler")
public class StoredRecordChunksKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LogManager.getLogger();
  private static final String INSTANCE_TITLE_FIELD_PATH = "title";
  public static final String STORED_RECORD_CHUNKS_KAFKA_HANDLER_UUID = "4d39ced7-9b67-4bdc-b232-343dbb5b8cef";

  private static final Map<RecordType, DataImportEventTypes> RECORD_TYPE_TO_EVENT_TYPE = Map.of(
    MARC_BIB, DI_SRS_MARC_BIB_RECORD_CREATED,
    MARC_AUTHORITY, DI_SRS_MARC_AUTHORITY_RECORD_CREATED,
    MARC_HOLDING, DI_SRS_MARC_HOLDING_RECORD_CREATED,
    EDIFACT, DI_EDIFACT_RECORD_CREATED
  );

  private RecordsPublishingService recordsPublishingService;
  private EventProcessedService eventProcessedService;
  private JournalService journalService;
  private MappingRuleCache mappingRuleCache;
  private Vertx vertx;

  public StoredRecordChunksKafkaHandler(@Autowired @Qualifier("recordsPublishingService") RecordsPublishingService recordsPublishingService,
                                        @Autowired @Qualifier("journalServiceProxy") JournalService journalService,
                                        @Autowired @Qualifier("eventProcessedService") EventProcessedService eventProcessedService,
                                        @Autowired MappingRuleCache mappingRuleCache,
                                        @Autowired Vertx vertx) {
    this.recordsPublishingService = recordsPublishingService;
    this.eventProcessedService = eventProcessedService;
    this.journalService = journalService;
    this.mappingRuleCache = mappingRuleCache;
    this.vertx = vertx;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
    String chunkId = okapiConnectionParams.getHeaders().get("chunkId");
    String chunkNumber = okapiConnectionParams.getHeaders().get("chunkNumber");
    String jobExecutionId = okapiConnectionParams.getHeaders().get("jobExecutionId");

    Event event = Json.decodeValue(record.value(), Event.class);

    try {
      return eventProcessedService.collectData(event.getId(), STORED_RECORD_CHUNKS_KAFKA_HANDLER_UUID, okapiConnectionParams.getTenantId())
        .compose(res -> {
          RecordsBatchResponse recordsBatchResponse = Json.decodeValue(event.getEventPayload(), RecordsBatchResponse.class);
          List<Record> storedRecords = recordsBatchResponse.getRecords();

          // we only know record type by inspecting the records, assuming records are homogeneous type and defaulting to previous static value
          DataImportEventTypes eventType = !storedRecords.isEmpty() && RECORD_TYPE_TO_EVENT_TYPE.containsKey(storedRecords.get(0).getRecordType())
            ? RECORD_TYPE_TO_EVENT_TYPE.get(storedRecords.get(0).getRecordType())
            : DI_SRS_MARC_BIB_RECORD_CREATED;

          LOGGER.debug("RecordsBatchResponse has been received, starting processing chunkId: {} chunkNumber: {} jobExecutionId: {}", chunkId, chunkNumber, jobExecutionId);
          saveCreatedRecordsInfoToDataImportLog(storedRecords, okapiConnectionParams.getTenantId());
          return recordsPublishingService.sendEventsWithRecords(storedRecords, jobExecutionId,
              okapiConnectionParams, eventType.value())
            .compose(b -> {
              LOGGER.debug("RecordsBatchResponse processing has been completed chunkId: {} chunkNumber: {} jobExecutionId: {}", chunkId, chunkNumber, jobExecutionId);
              return Future.succeededFuture(chunkId);
            }, th -> {
              LOGGER.error("RecordsBatchResponse processing has failed with errors chunkId: {} chunkNumber: {} jobExecutionId: {}", chunkId, chunkNumber, jobExecutionId, th);
              return Future.failedFuture(th);
            });
        });
    } catch (Exception e) {
      LOGGER.error("Can't process kafka record: ", e);
      return Future.failedFuture(e);
    }
  }

  private void saveCreatedRecordsInfoToDataImportLog(List<Record> storedRecords, String tenantId) {
    MappingRuleCacheKey cacheKey = new MappingRuleCacheKey(tenantId, storedRecords.get(0).getRecordType());
    mappingRuleCache.get(cacheKey).onComplete(rulesAr -> {
      if (rulesAr.succeeded()) {
        JsonArray journalRecords = buildJournalRecords(storedRecords, rulesAr.result(), tenantId);
        journalService.saveBatch(journalRecords, tenantId);
        return;
      }
      JsonArray journalRecords = buildJournalRecords(storedRecords, Optional.empty(), tenantId);
      journalService.saveBatch(journalRecords, tenantId);
    });
  }

  private JsonArray buildJournalRecords(List<Record> storedRecords, Optional<JsonObject> mappingRulesOptional, String tenantId) {
    EntityType entityType = getEntityType(storedRecords);
    JsonArray journalRecords = new JsonArray();

    String titleFieldTag = null;
    List<String> subfieldCodes = null;

    if (mappingRulesOptional.isPresent()) {
      JsonObject mappingRules = mappingRulesOptional.get();
      Optional<String> titleFieldOptional = getTitleFieldTagByInstanceFieldPath(mappingRules);

      if (titleFieldOptional.isPresent()) {
        titleFieldTag = titleFieldOptional.get();
        subfieldCodes = mappingRules.getJsonArray(titleFieldTag).stream()
          .map(JsonObject.class::cast)
          .filter(fieldMappingRule -> fieldMappingRule.getString("target").equals(INSTANCE_TITLE_FIELD_PATH))
          .flatMap(fieldMappingRule -> fieldMappingRule.getJsonArray("subfield").stream())
          .map(subfieldCode -> subfieldCode.toString())
          .collect(Collectors.toList());
      }
    }

    for (Record record : storedRecords) {
      JournalRecord journalRecord = new JournalRecord()
        .withJobExecutionId(record.getSnapshotId())
        .withSourceRecordOrder(record.getOrder())
        .withSourceId(record.getId())
        .withEntityType(entityType)
        .withEntityId(record.getId())
        .withActionType(CREATE)
        .withActionStatus(COMPLETED)
        .withActionDate(new Date())
        .withTitle(allNotNull(record.getParsedRecord(), titleFieldTag)
          ? ParsedRecordUtil.retrieveDataByField(record.getParsedRecord(), titleFieldTag, subfieldCodes) : null);

      journalRecords.add(JsonObject.mapFrom(journalRecord));
    }
    return journalRecords;
  }

  private EntityType getEntityType(List<Record> storedRecords) {
    switch (storedRecords.get(0).getRecordType()) {
      case EDIFACT:
        return EntityType.EDIFACT;
      case MARC_AUTHORITY:
        return EntityType.MARC_AUTHORITY;
      case MARC_HOLDING:
        return EntityType.MARC_HOLDINGS;
      case MARC_BIB:
      default:
        return EntityType.MARC_BIBLIOGRAPHIC;
    }
  }

  private Optional<String> getTitleFieldTagByInstanceFieldPath(JsonObject mappingRules) {
    return mappingRules.getMap().keySet().stream()
      .filter(fieldTag -> mappingRules.getJsonArray(fieldTag).stream()
        .map(o -> (JsonObject) o)
        .anyMatch(fieldMappingRule -> INSTANCE_TITLE_FIELD_PATH.equals(fieldMappingRule.getString("target"))))
      .findFirst();
  }

}
