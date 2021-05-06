package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.JournalRecord.EntityType;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.RecordType;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.services.MappingRuleCache;
import org.folio.services.RecordsPublishingService;
import org.folio.services.journal.JournalService;
import org.folio.services.util.ParsedRecordUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
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

@Component
@Qualifier("StoredRecordChunksKafkaHandler")
public class StoredRecordChunksKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LogManager.getLogger();
  private static final String INSTANCE_TITLE_FIELD_PATH = "title";

  private static final Map<RecordType, DataImportEventTypes> RECORD_TYPE_TO_EVENT_TYPE = Map.of(
    MARC_BIB, DI_SRS_MARC_BIB_RECORD_CREATED,
    MARC_AUTHORITY, DI_SRS_MARC_AUTHORITY_RECORD_CREATED,
    MARC_HOLDING, DI_SRS_MARC_HOLDING_RECORD_CREATED,
    EDIFACT, DI_EDIFACT_RECORD_CREATED
  );

  private RecordsPublishingService recordsPublishingService;
  private KafkaInternalCache kafkaInternalCache;
  private JournalService journalService;
  private MappingRuleCache mappingRuleCache;
  private Vertx vertx;

  public StoredRecordChunksKafkaHandler(@Autowired @Qualifier("recordsPublishingService") RecordsPublishingService recordsPublishingService,
                                        @Autowired @Qualifier("journalServiceProxy") JournalService journalService,
                                        @Autowired KafkaInternalCache kafkaInternalCache,
                                        @Autowired MappingRuleCache mappingRuleCache,
                                        @Autowired Vertx vertx) {
    this.recordsPublishingService = recordsPublishingService;
    this.journalService = journalService;
    this.kafkaInternalCache = kafkaInternalCache;
    this.mappingRuleCache = mappingRuleCache;
    this.vertx = vertx;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
    String correlationId = okapiConnectionParams.getHeaders().get("correlationId");
    String chunkNumber = okapiConnectionParams.getHeaders().get("chunkNumber");

    Event event = Json.decodeValue(record.value(), Event.class);

    if (!kafkaInternalCache.containsByKey(event.getId())) {
      try {
        kafkaInternalCache.putToCache(event.getId());
        RecordsBatchResponse recordsBatchResponse = Json.decodeValue(ZIPArchiver.unzip(event.getEventPayload()), RecordsBatchResponse.class);
        List<Record> storedRecords = recordsBatchResponse.getRecords();

        // we only know record type by inspecting the records, assuming records are homogeneous type and defaulting to previous static value
        DataImportEventTypes eventType = !storedRecords.isEmpty() && RECORD_TYPE_TO_EVENT_TYPE.containsKey(storedRecords.get(0).getRecordType())
          ? RECORD_TYPE_TO_EVENT_TYPE.get(storedRecords.get(0).getRecordType())
          : DI_SRS_MARC_BIB_RECORD_CREATED;

        LOGGER.debug("RecordsBatchResponse has been received, starting processing correlationId: {} chunkNumber: {}", correlationId, chunkNumber);
        saveCreatedRecordsInfoToDataImportLog(storedRecords, okapiConnectionParams.getTenantId());
        return recordsPublishingService.sendEventsWithRecords(storedRecords, okapiConnectionParams.getHeaders().get("jobExecutionId"),
          okapiConnectionParams, eventType.value())
          .compose(b -> {
            LOGGER.debug("RecordsBatchResponse processing has been completed correlationId: {} chunkNumber: {}", correlationId, chunkNumber);
            return Future.succeededFuture(correlationId);
          }, th -> {
            LOGGER.error("RecordsBatchResponse processing has failed with errors correlationId: {} chunkNumber: {}", correlationId, chunkNumber, th);
            return Future.failedFuture(th);
          });
      } catch (IOException e) {
        LOGGER.error("Can't process kafka record: ", e);
        return Future.failedFuture(e);
      }
    }
    return Future.succeededFuture(record.key());
  }

  private void saveCreatedRecordsInfoToDataImportLog(List<Record> storedRecords, String tenantId) {
    mappingRuleCache.get(tenantId).onComplete(rulesAr -> {
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
    EntityType entityType = storedRecords.get(0).getRecordType() == EDIFACT ? EntityType.EDIFACT : EntityType.MARC_BIBLIOGRAPHIC;
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

  private Optional<String> getTitleFieldTagByInstanceFieldPath(JsonObject mappingRules) {
    return mappingRules.getMap().keySet().stream()
      .filter(fieldTag -> mappingRules.getJsonArray(fieldTag).stream()
        .map(o -> (JsonObject) o)
        .anyMatch(fieldMappingRule -> INSTANCE_TITLE_FIELD_PATH.equals(fieldMappingRule.getString("target"))))
      .findFirst();
  }

}
