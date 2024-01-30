package org.folio.verticle.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.impl.future.FailedFuture;
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
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.JournalRecord.EntityType;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.RecordType;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.services.EventProcessedService;
import org.folio.services.JobExecutionService;
import org.folio.services.MappingRuleCache;
import org.folio.services.RecordsPublishingService;
import org.folio.services.entity.MappingRuleCacheKey;
import org.folio.services.journal.JournalService;
import org.folio.services.util.ParsedRecordUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.ws.rs.NotFoundException;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_EDIFACT_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_MARC_BIB_FOR_ORDER_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDING_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.COMPLETED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.CREATE;
import static org.folio.rest.jaxrs.model.Record.RecordType.EDIFACT;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_HOLDING;
import static org.folio.verticle.consumers.util.JobExecutionUtils.isNeedToSkip;
import static org.folio.verticle.consumers.util.MarcImportEventsHandler.NO_TITLE_MESSAGE;

@Component
@Qualifier("StoredRecordChunksKafkaHandler")
public class StoredRecordChunksKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LogManager.getLogger();
  private static final String INSTANCE_TITLE_FIELD_PATH = "title";
  public static final String STORED_RECORD_CHUNKS_KAFKA_HANDLER_UUID = "4d39ced7-9b67-4bdc-b232-343dbb5b8cef";
  public static final String ORDER_TYPE = "ORDER";
  public static final String FOLIO_RECORD = "folioRecord";
  public static final String ACTION_FIELD = "action";
  public static final String CREATE_ACTION = "CREATE";

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
  private JobExecutionService jobExecutionService;
  private Vertx vertx;

  public StoredRecordChunksKafkaHandler(@Autowired @Qualifier("recordsPublishingService") RecordsPublishingService recordsPublishingService,
                                        @Autowired @Qualifier("journalServiceProxy") JournalService journalService,
                                        @Autowired @Qualifier("eventProcessedService") EventProcessedService eventProcessedService,
                                        @Autowired JobExecutionService jobExecutionService,
                                        @Autowired MappingRuleCache mappingRuleCache,
                                        @Autowired Vertx vertx) {
    this.recordsPublishingService = recordsPublishingService;
    this.eventProcessedService = eventProcessedService;
    this.journalService = journalService;
    this.mappingRuleCache = mappingRuleCache;
    this.jobExecutionService = jobExecutionService;
    this.vertx = vertx;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
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

          Event event = Json.decodeValue(record.value(), Event.class);

          try {
            return eventProcessedService.collectData(STORED_RECORD_CHUNKS_KAFKA_HANDLER_UUID, event.getId(), okapiConnectionParams.getTenantId())
              .compose(res -> {
                RecordsBatchResponse recordsBatchResponse = Json.decodeValue(event.getEventPayload(), RecordsBatchResponse.class);
                List<Record> storedRecords = recordsBatchResponse.getRecords();

                // we only know record type by inspecting the records, assuming records are homogeneous type and defaulting to previous static value
                DataImportEventTypes eventType = !storedRecords.isEmpty() && RECORD_TYPE_TO_EVENT_TYPE.containsKey(storedRecords.get(0).getRecordType())
                  ? RECORD_TYPE_TO_EVENT_TYPE.get(storedRecords.get(0).getRecordType())
                  : DI_SRS_MARC_BIB_RECORD_CREATED;

                LOGGER.debug("handle:: RecordsBatchResponse has been received, starting processing chunkId: {} chunkNumber: {} jobExecutionId: {}", chunkId, chunkNumber, jobExecutionId);
                saveCreatedRecordsInfoToDataImportLog(storedRecords, okapiConnectionParams.getTenantId());
                return Future.succeededFuture(jobExecution)
                  .compose(jobExecutionFuture -> {
                    LOGGER.debug("handle:: JobExecution found by id {}: chunkId:{} chunkNumber: {} ", jobExecutionId, chunkId, chunkNumber);
                    return setOrderEventTypeIfNeeded(jobExecutionFuture, eventType);
                  })
                  .compose(eventTypes -> recordsPublishingService.sendEventsWithRecords(storedRecords, jobExecutionId,
                    okapiConnectionParams, eventTypes.value()))
                  .compose(b -> {
                    LOGGER.debug("handle:: RecordsBatchResponse processing has been completed chunkId: {} chunkNumber: {} jobExecutionId: {}", chunkId, chunkNumber, jobExecutionId);
                    return Future.succeededFuture(chunkId);
                  }, th -> {
                    LOGGER.warn("handle:: RecordsBatchResponse processing has failed with errors chunkId: {} chunkNumber: {} jobExecutionId: {}", chunkId, chunkNumber, jobExecutionId, th);
                    return Future.failedFuture(th);
                  });
              });
          } catch (Exception e) {
            LOGGER.warn("handle:: Can't process kafka record: ", e);
            return new FailedFuture<String>(e);
          }
        })
        .orElseGet(() -> {
          LOGGER.warn("handle:: Couldn't find JobExecution by id {}: chunkId:{} chunkNumber: {} ", jobExecutionId, chunkId, chunkNumber);
          return Future.failedFuture(new NotFoundException(format("Couldn't find JobExecution with id %s chunkId:%s chunkNumber: %s", jobExecutionId, chunkId, chunkNumber)));
        }));
  }

  private Future<DataImportEventTypes> setOrderEventTypeIfNeeded(JobExecution jobExecution, DataImportEventTypes dataImportEventTypes) {
    if (jobExecution.getJobProfileSnapshotWrapper() != null) {
      ProfileSnapshotWrapper profileSnapshotWrapper = new ObjectMapper().convertValue(jobExecution.getJobProfileSnapshotWrapper(), ProfileSnapshotWrapper.class);
      List<ProfileSnapshotWrapper> actionProfiles = profileSnapshotWrapper
        .getChildSnapshotWrappers()
        .stream()
        .filter(e -> e.getContentType() == ProfileSnapshotWrapper.ContentType.ACTION_PROFILE)
        .collect(Collectors.toList());

      if (!actionProfiles.isEmpty() && checkIfOrderCreateActionProfileExists(actionProfiles)) {
        dataImportEventTypes = DI_MARC_BIB_FOR_ORDER_CREATED;
        LOGGER.debug("setOrderEventTypeIfNeeded:: Event type for Order's logic set by jobExecutionId {} ", jobExecution.getId());
      }
    }
    return Future.succeededFuture(dataImportEventTypes);
  }

  private static boolean checkIfOrderCreateActionProfileExists(List<ProfileSnapshotWrapper> actionProfiles) {
    for (ProfileSnapshotWrapper actionProfile : actionProfiles) {
      LinkedHashMap<String, String> content = new ObjectMapper().convertValue(actionProfile.getContent(), LinkedHashMap.class);
      if (content.get(FOLIO_RECORD).equals(ORDER_TYPE) && content.get(ACTION_FIELD).equals(CREATE_ACTION)) {
        return true;
      }
    }
    return false;
  }

  private void saveCreatedRecordsInfoToDataImportLog(List<Record> storedRecords, String tenantId) {
    MappingRuleCacheKey cacheKey = new MappingRuleCacheKey(tenantId, storedRecords.get(0).getRecordType());
    mappingRuleCache.get(cacheKey).onComplete(rulesAr -> {
      if (rulesAr.succeeded()) {
        JsonArray journalRecords = buildJournalRecords(storedRecords, rulesAr.result());
        journalService.saveBatch(journalRecords, tenantId);
        return;
      }
      JsonArray journalRecords = buildJournalRecords(storedRecords, Optional.empty());
      journalService.saveBatch(journalRecords, tenantId);
    });
  }

  private JsonArray buildJournalRecords(List<Record> storedRecords, Optional<JsonObject> mappingRulesOptional) {
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

      if (record.getErrorRecord() == null) {
        String retrievedTitleFromRecord = getTitleFromRecord(record, titleFieldTag, subfieldCodes);
        if (retrievedTitleFromRecord != null && retrievedTitleFromRecord.isEmpty()) retrievedTitleFromRecord = NO_TITLE_MESSAGE;

        JournalRecord journalRecord = new JournalRecord()
          .withJobExecutionId(record.getSnapshotId())
          .withSourceRecordOrder(record.getOrder())
          .withSourceId(record.getId())
          .withEntityType(entityType)
          .withEntityId(record.getId())
          .withActionType(CREATE)
          .withActionStatus(COMPLETED)
          .withActionDate(new Date())
          .withTitle(retrievedTitleFromRecord);

        journalRecords.add(JsonObject.mapFrom(journalRecord));
      }
    }
    return journalRecords;
  }

  private static String getTitleFromRecord(Record record, String titleFieldTag, List<String> subfieldCodes) {
    return titleFieldTag != null ? ParsedRecordUtil.retrieveDataByField(record.getParsedRecord(), titleFieldTag, subfieldCodes) : null;
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
