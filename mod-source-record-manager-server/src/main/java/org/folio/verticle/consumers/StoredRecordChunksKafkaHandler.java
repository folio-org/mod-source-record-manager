package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.impl.future.FailedFuture;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
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
import org.folio.services.JobExecutionService;
import org.folio.services.MappingRuleCache;
import org.folio.services.RecordsPublishingService;
import org.folio.services.entity.MappingRuleCacheKey;
import org.folio.services.journal.BatchableJournalRecord;
import org.folio.services.util.ParsedRecordUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INCOMING_EDIFACT_RECORD_PARSED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDING_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.COMPLETED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.CREATE;
import static org.folio.rest.jaxrs.model.Record.RecordType.EDIFACT;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_HOLDING;
import static org.folio.services.journal.JournalUtil.getJournalMessageProducer;
import static org.folio.verticle.consumers.util.JobExecutionUtils.isNeedToSkip;
import static org.folio.verticle.consumers.util.MarcImportEventsHandler.NO_TITLE_MESSAGE;

@Component
@Qualifier("StoredRecordChunksKafkaHandler")
public class StoredRecordChunksKafkaHandler implements AsyncRecordHandler<String, byte[]> {
  private static final Logger LOGGER = LogManager.getLogger();
  private static final String INSTANCE_TITLE_FIELD_PATH = "title";
  public static final String STORED_RECORD_CHUNKS_KAFKA_HANDLER_UUID = "4d39ced7-9b67-4bdc-b232-343dbb5b8cef";
  public static final String ORDER_TYPE = "ORDER";
  public static final String FOLIO_RECORD = "folioRecord";
  public static final String ACTION_FIELD = "action";
  public static final String CREATE_ACTION = "CREATE";

  private static final Map<RecordType, DataImportEventTypes> RECORD_TYPE_TO_EVENT_TYPE = Map.of(
    MARC_BIB, DI_INCOMING_MARC_BIB_RECORD_PARSED,
    MARC_AUTHORITY, DI_SRS_MARC_AUTHORITY_RECORD_CREATED,
    MARC_HOLDING, DI_SRS_MARC_HOLDING_RECORD_CREATED,
    EDIFACT, DI_INCOMING_EDIFACT_RECORD_PARSED
  );

  private RecordsPublishingService recordsPublishingService;
  private EventProcessedService eventProcessedService;
  private MappingRuleCache mappingRuleCache;
  private JobExecutionService jobExecutionService;
  private Vertx vertx;
  private MessageProducer<Collection<BatchableJournalRecord>> journalRecordProducer;

  public StoredRecordChunksKafkaHandler(@Autowired @Qualifier("recordsPublishingService") RecordsPublishingService recordsPublishingService,
                                        @Autowired @Qualifier("eventProcessedService") EventProcessedService eventProcessedService,
                                        @Autowired JobExecutionService jobExecutionService,
                                        @Autowired MappingRuleCache mappingRuleCache,
                                        @Autowired Vertx vertx) {
    this.recordsPublishingService = recordsPublishingService;
    this.eventProcessedService = eventProcessedService;
    this.mappingRuleCache = mappingRuleCache;
    this.jobExecutionService = jobExecutionService;
    this.vertx = vertx;
    this.journalRecordProducer = getJournalMessageProducer(vertx);
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, byte[]> record) {
    List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
    String chunkId = okapiConnectionParams.getHeaders().get("chunkId");
    String chunkNumber = okapiConnectionParams.getHeaders().get("chunkNumber");
    String jobExecutionId = okapiConnectionParams.getHeaders().get("jobExecutionId");

    return jobExecutionService.getJobExecutionById(jobExecutionId, okapiConnectionParams.getTenantId())
      .compose(jobExecutionOptional -> jobExecutionOptional.map(jobExecution -> {

          if (isNeedToSkip(jobExecution)) {
            LOGGER.info("handle:: do not handle because jobExecution with id: {} was cancelled", jobExecutionId);
            return Future.succeededFuture(chunkId);
          }

          try {
            Event event = DatabindCodec.mapper().readValue(record.value(), Event.class);
            return eventProcessedService.collectData(STORED_RECORD_CHUNKS_KAFKA_HANDLER_UUID, event.getId(), okapiConnectionParams.getTenantId())
              .compose(res -> {
                RecordsBatchResponse recordsBatchResponse = Json.decodeValue(event.getEventPayload(), RecordsBatchResponse.class);
                List<Record> storedRecords = recordsBatchResponse.getRecords();

                // we only know record type by inspecting the records, assuming records are homogeneous type and defaulting to previous static value
                DataImportEventTypes eventType = !storedRecords.isEmpty() && RECORD_TYPE_TO_EVENT_TYPE.containsKey(storedRecords.get(0).getRecordType())
                  ? RECORD_TYPE_TO_EVENT_TYPE.get(storedRecords.get(0).getRecordType())
                  : DI_INCOMING_MARC_BIB_RECORD_PARSED;

                LOGGER.debug("handle:: RecordsBatchResponse has been received, starting processing chunkId: {} chunkNumber: {} jobExecutionId: {}", chunkId, chunkNumber, jobExecutionId);
                saveCreatedRecordsInfoToDataImportLog(storedRecords, okapiConnectionParams.getTenantId());
                return recordsPublishingService.sendEventsWithRecords(storedRecords, jobExecutionId, okapiConnectionParams, eventType.value(), null)
                  .compose(b -> {
                    LOGGER.debug("handle:: RecordsBatchResponse processing has been completed chunkId: {} chunkNumber: {} jobExecutionId: {}", chunkId, chunkNumber, jobExecutionId);
                    return Future.succeededFuture(chunkId);
                  }, th -> {
                    LOGGER.warn("handle:: RecordsBatchResponse processing has failed with errors chunkId: {} chunkNumber: {} jobExecutionId: {}", chunkId, chunkNumber, jobExecutionId, th);
                    return Future.failedFuture(th);
                  });
              });
          } catch (Exception e) {
            LOGGER.warn("handle:: Can't process kafka record, jobExecutionId: {}", jobExecutionId, e);
            return new FailedFuture<String>(e);
          }
        })
        .orElseGet(() -> {
          LOGGER.warn("handle:: Couldn't find JobExecution by id {}: chunkId:{} chunkNumber: {} ", jobExecutionId, chunkId, chunkNumber);
          return Future.failedFuture(new NotFoundException(format("Couldn't find JobExecution with id %s chunkId:%s chunkNumber: %s", jobExecutionId, chunkId, chunkNumber)));
        }));
  }

  private void saveCreatedRecordsInfoToDataImportLog(List<Record> storedRecords, String tenantId) {
    MappingRuleCacheKey cacheKey = new MappingRuleCacheKey(tenantId, storedRecords.get(0).getRecordType());

    mappingRuleCache.get(cacheKey).onComplete(rulesAr -> {
      if (rulesAr.succeeded()) {
        Collection<BatchableJournalRecord> journalRecords = buildJournalRecords(storedRecords, rulesAr.result(), tenantId);
        journalRecordProducer.write(journalRecords);
        return;
      }
      Collection<BatchableJournalRecord> journalRecords = buildJournalRecords(storedRecords, Optional.empty(), tenantId);
      journalRecordProducer.write(journalRecords);
    });
  }

  private Collection<BatchableJournalRecord> buildJournalRecords(List<Record> storedRecords, Optional<JsonObject> mappingRulesOptional, String tenantId) {
    EntityType entityType = getEntityType(storedRecords);
    List<BatchableJournalRecord> journalRecords = new ArrayList<>();

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
          .map(Object::toString)
          .collect(Collectors.toList());
      }
    }

    for (Record record : storedRecords) {

      if (record.getErrorRecord() == null) {
        String retrievedTitleFromRecord = getTitleFromRecord(record, titleFieldTag, subfieldCodes);
        if (retrievedTitleFromRecord != null && retrievedTitleFromRecord.isEmpty()) retrievedTitleFromRecord = NO_TITLE_MESSAGE;

        BatchableJournalRecord journalRecord = new BatchableJournalRecord(new JournalRecord()
          .withJobExecutionId(record.getSnapshotId())
          .withSourceRecordOrder(record.getOrder())
          .withSourceId(record.getId())
          .withEntityType(entityType)
          .withEntityId(record.getId())
          .withActionType(CREATE)
          .withActionStatus(COMPLETED)
          .withActionDate(new Date())
          .withTitle(retrievedTitleFromRecord), tenantId);

        journalRecords.add(journalRecord);
      }
    }
    return journalRecords;
  }

  private static String getTitleFromRecord(Record record, String titleFieldTag, List<String> subfieldCodes) {
    return titleFieldTag != null ? ParsedRecordUtil.retrieveDataByField(record.getParsedRecord(), titleFieldTag, subfieldCodes) : null;
  }

  private EntityType getEntityType(List<Record> storedRecords) {
    return switch (storedRecords.get(0).getRecordType()) {
      case EDIFACT -> EntityType.EDIFACT;
      case MARC_AUTHORITY -> EntityType.MARC_AUTHORITY;
      case MARC_HOLDING -> EntityType.MARC_HOLDINGS;
      default -> EntityType.MARC_BIBLIOGRAPHIC;
    };
  }

  private Optional<String> getTitleFieldTagByInstanceFieldPath(JsonObject mappingRules) {
    return mappingRules.getMap().keySet().stream()
      .filter(fieldTag -> mappingRules.getJsonArray(fieldTag).stream()
        .map(o -> (JsonObject) o)
        .anyMatch(fieldMappingRule -> INSTANCE_TITLE_FIELD_PATH.equals(fieldMappingRule.getString("target"))))
      .findFirst();
  }

}
