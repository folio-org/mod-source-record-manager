package org.folio.services;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INCOMING_EDIFACT_RECORD_PARSED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INCOMING_MARC_BIB_FOR_ORDER_PARSED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_MARC_FOR_DELETE_RECEIVED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_MARC_FOR_UPDATE_RECEIVED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_PARSED;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_HOLDING;
import static org.folio.services.JobProfileSnapshotInspector.hasCreateAuthorityAction;
import static org.folio.services.JobProfileSnapshotInspector.hasCreateInstanceActionWithoutPrecedingDelete999Field;
import static org.folio.services.JobProfileSnapshotInspector.hasCreateMarcHoldingsAction;
import static org.folio.services.JobProfileSnapshotInspector.hasCreateOrUpdateItemOrHoldingsAction;
import static org.folio.services.JobProfileSnapshotInspector.hasCreateOrderAction;
import static org.folio.services.JobProfileSnapshotInspector.hasDeleteMarcAuthorityAction;
import static org.folio.services.JobProfileSnapshotInspector.hasMarcAuthorityMatchProfile;
import static org.folio.services.JobProfileSnapshotInspector.hasUpdateInstanceAction;
import static org.folio.services.JobProfileSnapshotInspector.hasUpdateMarcAction;
import static org.folio.services.JobProfileSnapshotInspector.shouldRemoveSubfield9;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.INDICATOR_F;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.SUBFIELD_I;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.SUBFIELD_S;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.TAG_999;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.addFieldToMarcRecord;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.getControlFieldValue;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.getValue;
import static org.folio.services.journal.JournalUtil.getJournalMessageProducer;
import static org.folio.services.util.EventHandlingUtil.JOB_EXECUTION_ID_HEADER;
import static org.folio.services.util.EventHandlingUtil.USER_ID_HEADER;
import static org.folio.services.util.EventHandlingUtil.sendEventToKafka;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.impl.KafkaHeaderImpl;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import javax.ws.rs.NotFoundException;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.dataimport.util.marc.MarcRecordAnalyzer;
import org.folio.dataimport.util.marc.MarcRecordType;
import org.folio.dataimport.util.marc.RecordAnalyzer;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.JobProfileInfo.DataType;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.RecordType;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsMetadata;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.afterprocessing.FieldModificationService;
import org.folio.services.afterprocessing.HrIdFieldService;
import org.folio.services.exceptions.InvalidJobProfileForFileException;
import org.folio.services.journal.BatchableJournalRecord;
import org.folio.services.journal.JournalUtil;
import org.folio.services.parsers.ParsedResult;
import org.folio.services.parsers.RecordParserBuilder;
import org.folio.services.validation.JobProfileSnapshotValidationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Log4j2
@Service
public class ChangeEngineServiceImpl implements ChangeEngineService {

  private static final String MESSAGE_KEY = "message";
  private static final String TAG_001 = "001";
  private static final String TAG_004 = "004";
  private static final String MARC_FORMAT = "MARC_";
  private static final String INSTANCE_CREATION_999_ERROR_MESSAGE =
    "A new Instance was not created because the incoming record already contained a 999ff$s or 999ff$i field";
  private static final String HOLDINGS_CREATION_999_ERROR_MESSAGE =
    "A new MARC-Holding was not created because the incoming record already contained a 999ff$s or 999ff$i field";
  private static final String AUTHORITY_CREATION_999_ERROR_MESSAGE =
    "A new MARC-Authority was not created because the incoming record already contained a 999ff$s or 999ff$i field";
  private static final String WRONG_JOB_PROFILE_ERROR_MESSAGE =
    "Chosen job profile '%s' does not support '%s' record type";
  private static final String JOB_PROFILE_HAS_NO_CHILD_PROFILES_ERROR_MESSAGE =
    "The '%s' job profile does not have any linked action or matching profiles";
  private static final String ACCEPT_INSTANCE_ID_KEY = "acceptInstanceId";

  private final JobExecutionSourceChunkDao jobExecutionSourceChunkDao;
  private final JobExecutionService jobExecutionService;
  private final RecordAnalyzer marcRecordAnalyzer;
  private final HrIdFieldService hrIdFieldService;
  private final RecordsPublishingService recordsPublishingService;
  private final MappingMetadataService mappingMetadataService;
  private final JobProfileSnapshotValidationService jobProfileSnapshotValidationService;
  private final KafkaConfig kafkaConfig;
  private final FieldModificationService fieldModificationService;
  private final IncomingRecordService incomingRecordService;
  private final MarcHoldingsVerifier marcHoldingsVerifier;
  private final DistributionKeyGenerator distributionKeyGenerator;
  private final JobExecutionStatusCache jobExecutionStatusCache;
  private final MessageProducer<Collection<BatchableJournalRecord>> journalRecordProducer;
  private final ConcurrentHashMap<String, Future<Boolean>> ensuringInProgress = new ConcurrentHashMap<>();

  @Value("${chunk.processing.threshold.chunk.size:100}")
  private int thresholdChunkSize;

  public ChangeEngineServiceImpl(@Autowired JobExecutionSourceChunkDao jobExecutionSourceChunkDao,
                                 @Autowired JobExecutionService jobExecutionService,
                                 @Autowired MarcRecordAnalyzer marcRecordAnalyzer,
                                 @Autowired HrIdFieldService hrIdFieldService,
                                 @Autowired RecordsPublishingService recordsPublishingService,
                                 @Autowired MappingMetadataService mappingMetadataService,
                                 @Autowired JobProfileSnapshotValidationService jobProfileSnapshotValidationService,
                                 @Autowired KafkaConfig kafkaConfig,
                                 @Autowired FieldModificationService fieldModificationService,
                                 @Autowired IncomingRecordService incomingRecordService,
                                 @Autowired MarcHoldingsVerifier marcHoldingsVerifier,
                                 @Autowired DistributionKeyGenerator distributionKeyGenerator,
                                 @Autowired JobExecutionStatusCache jobExecutionStatusCache,
                                 @Autowired Vertx vertx) {
    this.jobExecutionSourceChunkDao = jobExecutionSourceChunkDao;
    this.jobExecutionService = jobExecutionService;
    this.marcRecordAnalyzer = marcRecordAnalyzer;
    this.hrIdFieldService = hrIdFieldService;
    this.recordsPublishingService = recordsPublishingService;
    this.mappingMetadataService = mappingMetadataService;
    this.jobProfileSnapshotValidationService = jobProfileSnapshotValidationService;
    this.kafkaConfig = kafkaConfig;
    this.fieldModificationService = fieldModificationService;
    this.incomingRecordService = incomingRecordService;
    this.marcHoldingsVerifier = marcHoldingsVerifier;
    this.distributionKeyGenerator = distributionKeyGenerator;
    this.jobExecutionStatusCache = jobExecutionStatusCache;
    this.journalRecordProducer = getJournalMessageProducer(vertx);
  }

  @Override
  public Future<List<Record>> parseRawRecordsChunkForJobExecution(RawRecordsDto chunk, JobExecution jobExecution,
                                                                  String sourceChunkId, boolean acceptInstanceId,
                                                                  ConnectionParams params) {
    Promise<List<Record>> promise = Promise.promise();
    Future<List<Record>> futureParsedRecords =
      parseRecords(chunk.getInitialRecords(), chunk.getRecordsMetadata().getContentType(), jobExecution, sourceChunkId,
        params.getTenantId(), acceptInstanceId, params);

    futureParsedRecords
      .map(parsedRecords -> {
        saveIncomingAndJournalRecords(parsedRecords, params.getTenantId());
        return parsedRecords;
      })
      .compose(parsedRecords -> validateJobProfile(jobExecution, parsedRecords).map(parsedRecords))
      .compose(parsedRecords -> marcHoldingsVerifier.filterValidRecords(jobExecution, params, parsedRecords))
      .compose(
        filteredParsedRecords -> ensureMappingMetaDataSnapshot(jobExecution.getId(), filteredParsedRecords, params)
          .map(filteredParsedRecords))
      .onSuccess(parsedRecords -> {
        fillParsedRecordsWithAdditionalFields(parsedRecords);
        processRecords(parsedRecords, jobExecution, params, sourceChunkId, acceptInstanceId, promise);
      }).onFailure(th -> {
        log.warn("parseRawRecordsChunkForJobExecution:: Error parsing records, jobExecutionId: {}",
          jobExecution.getId(), th);
        promise.fail(th);
      });
    return promise.future();
  }

  @Override
  public List<Record> getParsedRecordsFromInitialRecords(List<InitialRecord> rawRecords,
                                                         RecordsMetadata.ContentType recordContentType,
                                                         JobExecution jobExecution,
                                                         boolean acceptInstanceId,
                                                         String sourceChunkId) {
    log.debug(
      "getParsedRecordsFromInitialRecords:: recordContentType: {}, jobExecutionId: {}, acceptInstanceId: {}, sourceChunkId: {}",
      recordContentType, jobExecution.getId(), acceptInstanceId, sourceChunkId);

    var parser = RecordParserBuilder.buildParser(recordContentType);

    return rawRecords.stream()
      .map(rawRecord -> {
        var parsedResult = parser.parseRecord(rawRecord.getRecord());

        if (!acceptInstanceId) {
          parsedResult = addErrorMessageWhen999ffFieldExistsOnCreateAction(jobExecution, parsedResult);
        } else {
          log.debug(
            "getParsedRecordsFromInitialRecords:: acceptInstanceId = true, sourceChunkId = {}, jobExecutionId = {} ",
            sourceChunkId, jobExecution.getId());
        }

        var recordId = UUID.randomUUID().toString();
        var record = new Record()
          .withId(recordId)
          .withRecordType(inferRecordType(jobExecution, parsedResult, recordId, sourceChunkId))
          .withSnapshotId(jobExecution.getId())
          .withOrder(rawRecord.getOrder())
          .withState(Record.State.ACTUAL)
          .withRawRecord(new RawRecord().withContent(rawRecord.getRecord()));
        if (parsedResult.isHasError()) {
          log.warn(
            "getParsedRecordsFromInitialRecords:: Parsed record with order: {} contains errors: {}, jobExecutionId: {}, sourceChunkId: {}",
            rawRecord.getOrder(), parsedResult.getErrors().encode(), jobExecution.getId(), sourceChunkId);
          record.setErrorRecord(new ErrorRecord()
            .withContent(rawRecord)
            .withDescription(parsedResult.getErrors().encode()));
        } else {
          record.setParsedRecord(
            new ParsedRecord().withId(recordId).withContent(parsedResult.getParsedRecord().encode()));
          if (jobExecution.getJobProfileInfo().getDataType().equals(DataType.MARC)) {
            postProcessMarcRecord(record, rawRecord);
          }
        }
        return record;
      }).toList();
  }

  private Future<Void> validateJobProfile(JobExecution jobExecution, List<Record> records) {
    ProfileSnapshotWrapper jobProfileSnapshot = jobExecution.getJobProfileSnapshotWrapper();
    if (CollectionUtils.isEmpty(jobProfileSnapshot.getChildSnapshotWrappers())) {
      return Future.failedFuture(new InvalidJobProfileForFileException(
        records,
        String.format(JOB_PROFILE_HAS_NO_CHILD_PROFILES_ERROR_MESSAGE, jobExecution.getJobProfileInfo().getName())));
    }

    if (isJobProfileCompatibleWithRecordsType(jobProfileSnapshot, records)) {
      return Future.succeededFuture();
    }
    jobExecutionStatusCache.markAsError(jobExecution.getId());
    return Future.failedFuture(new InvalidJobProfileForFileException(records,
      prepareWrongJobProfileErrorMessage(jobExecution, records)));
  }

  private void processRecords(List<Record> parsedRecords, JobExecution jobExecution, ConnectionParams params,
                              String sourceChunkId, boolean acceptInstanceId, Promise<List<Record>> promise) {
    var action = getAction(parsedRecords, jobExecution);
    log.debug(
      "processRecords:: Starting to process parsed records for jobExecutionId: {}, sourceChunkId: {}, action determined by job profile: {}",
      jobExecution.getId(), sourceChunkId, action);

    if (action == ActionType.SAVE_RECORD) {
      saveRecords(jobExecution, sourceChunkId, params, parsedRecords, promise);
      return;
    }
    publishForAction(action, parsedRecords, jobExecution, params, acceptInstanceId)
      .onSuccess(ar -> promise.complete(parsedRecords))
      .onFailure(promise::fail);
  }

  private Future<Boolean> publishForAction(ActionType action, List<Record> parsedRecords, JobExecution jobExecution,
                                           ConnectionParams params, boolean acceptInstanceId) {
    return switch (action) {
      case UPDATE_RECORD -> {
        hrIdFieldService.move001valueTo035Field(parsedRecords);
        yield updateRecords(parsedRecords, jobExecution, params);
      }
      case DELETE_RECORD -> deleteRecords(parsedRecords, jobExecution, params);
      case CREATE_ORDER -> sendEvents(parsedRecords, jobExecution, params, DI_INCOMING_MARC_BIB_FOR_ORDER_PARSED);
      case SEND_ERROR -> sendEvents(parsedRecords, jobExecution, params, DI_ERROR);
      case SEND_MARC_BIB -> sendEventWithContext(parsedRecords, jobExecution, params,
        Map.of(ACCEPT_INSTANCE_ID_KEY, Boolean.toString(acceptInstanceId)));
      case SEND_EDIFACT -> sendEvents(parsedRecords, jobExecution, params, DI_INCOMING_EDIFACT_RECORD_PARSED);
      case SAVE_RECORD -> throw new IllegalStateException("SAVE_RECORD is handled by saveRecords");
    };
  }

  private ActionType getAction(List<Record> parsedRecords, JobExecution jobExecution) {
    if (hasDeleteMarcAuthorityAction(jobExecution)) {
      return ActionType.DELETE_RECORD;
    }
    if (hasUpdateMarcAction(jobExecution) || hasUpdateInstanceAction(jobExecution)
        || hasCreateOrUpdateItemOrHoldingsAction(jobExecution, parsedRecords) || hasMarcAuthorityMatchProfile(
      jobExecution)) {
      return ActionType.UPDATE_RECORD;
    }
    if (hasCreateOrderAction(jobExecution)) {
      return ActionType.CREATE_ORDER;
    }
    if (parsedRecords.isEmpty()) {
      return ActionType.SAVE_RECORD;
    }
    RecordType recordType = parsedRecords.getFirst().getRecordType();
    if (recordType == RecordType.MARC_BIB) {
      return ActionType.SEND_MARC_BIB;
    }
    if (recordType == RecordType.EDIFACT) {
      return ActionType.SEND_EDIFACT;
    }
    if (recordType == null) {
      return ActionType.SEND_ERROR;
    }
    return ActionType.SAVE_RECORD;
  }

  private void saveRecords(JobExecution jobExecution, String sourceChunkId, ConnectionParams params,
                           List<Record> parsedRecords, Promise<List<Record>> promise) {
    saveRecords(params, jobExecution, parsedRecords)
      .onComplete(postAr -> {
        if (postAr.failed()) {
          StatusDto statusDto = new StatusDto()
            .withStatus(StatusDto.Status.ERROR)
            .withErrorStatus(StatusDto.ErrorStatus.RECORD_UPDATE_ERROR);
          jobExecutionService.updateJobExecutionStatus(jobExecution.getId(), statusDto, params)
            .onFailure(e -> log.warn(
              "parseRawRecordsChunkForJobExecution:: Error during update jobExecution with id '{}' and snapshot status",
              jobExecution.getId(), e));
          SourceChunkStateUpdater.markAsError(jobExecutionSourceChunkDao, sourceChunkId, jobExecution.getId(), params.getTenantId())
            .onComplete(ar -> promise.fail(postAr.cause()));
        } else {
          promise.complete(parsedRecords);
        }
      });
  }

  private Future<Boolean> sendEvents(List<Record> records, JobExecution jobExecution, ConnectionParams params,
                                     DataImportEventTypes eventType) {
    log.info("sendEvents:: Sending events with type: {}, jobExecutionId: {}", eventType.value(), jobExecution.getId());
    return recordsPublishingService.sendEventsWithRecords(records, jobExecution.getId(), params, eventType.value(),
      null);
  }

  private Future<Boolean> sendEventWithContext(List<Record> records, JobExecution jobExecution, ConnectionParams params,
                                               Map<String, String> eventContext) {
    log.info("sendEventWithContext:: Sending events with type: {}, jobExecutionId: {}, event context: {}",
      DI_INCOMING_MARC_BIB_RECORD_PARSED, jobExecution.getId(), eventContext);
    return recordsPublishingService.sendEventsWithRecords(records, jobExecution.getId(), params,
      DI_INCOMING_MARC_BIB_RECORD_PARSED.value(), eventContext);
  }

  private void saveIncomingAndJournalRecords(List<Record> parsedRecords, String tenantId) {
    if (!parsedRecords.isEmpty()) {
      incomingRecordService.saveBatch(JournalUtil.buildIncomingRecordsByRecords(parsedRecords), tenantId);
      List<BatchableJournalRecord> batchableJournalRecords = JournalUtil.buildJournalRecordsByRecords(parsedRecords)
        .stream()
        .map(r -> new BatchableJournalRecord(r, tenantId))
        .toList();
      journalRecordProducer.write(batchableJournalRecords);
    }
  }

  /**
   * Checks whether job profile snapshot is compatible with record type of the specified {@code records}.
   * Returns {@code true} for the specified records that have not been parsed successfully and therefore
   * which have the recordType == null to store them in the record-storage with the corresponding parsing error message.
   *
   * @param jobProfileSnapshot job profile snapshot
   * @param records            parsed source records
   * @return {@code true} if the specified job profile snapshot is compatible with type of the {@code records}, otherwise {@code false}
   */
  private boolean isJobProfileCompatibleWithRecordsType(ProfileSnapshotWrapper jobProfileSnapshot,
                                                        List<Record> records) {
    if (records.isEmpty()) {
      return true;
    }
    RecordType recordType = records.getFirst().getRecordType();
    return recordType == null || jobProfileSnapshotValidationService.isJobProfileCompatibleWithRecordType(
      jobProfileSnapshot, recordType);
  }

  private Future<Boolean> updateRecords(List<Record> records, JobExecution jobExecution, ConnectionParams params) {
    log.info(
      "updateRecords:: Records have not been saved in record-storage, because job contains action for Marc or Instance update");
    return recordsPublishingService
      .sendEventsWithRecords(records, jobExecution.getId(), params, DI_MARC_FOR_UPDATE_RECEIVED.value(), null);
  }

  private Future<Boolean> deleteRecords(List<Record> records, JobExecution jobExecution, ConnectionParams params) {
    log.info(
      "deleteRecords:: Records have not been saved in record-storage, because job contains action for Marc delete");
    return recordsPublishingService
      .sendEventsWithRecords(records, jobExecution.getId(), params, DI_MARC_FOR_DELETE_RECEIVED.value(), null);
  }

  private Future<Boolean> ensureMappingMetaDataSnapshot(String jobExecutionId, List<Record> recordsList,
                                                        ConnectionParams okapiParams) {
    if (CollectionUtils.isEmpty(recordsList)) {
      return Future.succeededFuture(false);
    }

    Future<Boolean> operationFuture = ensuringInProgress.compute(jobExecutionId, (key, existingFuture) -> {
      if (existingFuture != null && !existingFuture.isComplete()) {
        log.debug(
          "ensureMappingMetaDataSnapshot:: Joining an already in-progress ensure operation for jobExecutionId: {}",
          key);
        return existingFuture;
      }

      log.debug("ensureMappingMetaDataSnapshot:: Starting a new ensure operation for jobExecutionId: {}", key);
      return mappingMetadataService.getMappingMetadataDto(key, okapiParams)
        .map(dto -> {
          log.debug("ensureMappingMetaDataSnapshot:: Snapshots already exist for jobExecutionId: {}, size: {}",
            key, recordsList.size());
          return false;
        })
        .recover(throwable -> {
          NotFoundException notFoundEx = extractNotFoundException(throwable);
          if (notFoundEx != null) {
            log.debug("ensureMappingMetaDataSnapshot:: Snapshots not found for jobExecutionId: '{}'. Creating them...",
              key);
            RecordType recordType = recordsList.getFirst().getRecordType();
            recordType = Objects.isNull(recordType) || recordType == RecordType.EDIFACT ? MARC_BIB : recordType;
            return mappingMetadataService.saveMappingRulesSnapshot(key, recordType.toString(),
                okapiParams.getTenantId())
              .compose(arMappingRules -> mappingMetadataService.saveMappingParametersSnapshot(key, okapiParams))
              .map(ar -> {
                log.info(
                  "ensureMappingMetaDataSnapshot:: MappingRules and MappingParameters snapshots were saved successfully for jobExecutionId: {}, size: {}",
                  key, recordsList.size());
                return true;
              });
          } else {
            log.error(
              "ensureMappingMetaDataSnapshot:: An unexpected error occurred while checking for snapshots for jobExecutionId: {}",
              key, throwable);
            return Future.failedFuture(throwable);
          }
        });
    });

    operationFuture.onComplete(ar -> {
      log.debug("ensureMappingMetaDataSnapshot:: Completed ensure operation for jobExecutionId: {}", jobExecutionId);
      ensuringInProgress.remove(jobExecutionId);
    });

    return operationFuture;
  }

  private NotFoundException extractNotFoundException(Throwable throwable) {
    return throwable instanceof NotFoundException notFoundEx ? notFoundEx : null;
  }

  /**
   * Parse list of source records
   *
   * @param rawRecords       - list of raw records for parsing
   * @param jobExecution     - job execution of record's parsing
   * @param sourceChunkId    - id of the JobExecutionSourceChunk
   * @param tenantId         - tenant id
   * @param acceptInstanceId - allow the 999ff$i field to be set and also create an instance with value in 999ff$i
   * @param okapiParams      - ConnectionParams to interact with external services
   * @return - list of records with parsed or error data
   */
  private Future<List<Record>> parseRecords(List<InitialRecord> rawRecords,
                                            RecordsMetadata.ContentType recordContentType,
                                            JobExecution jobExecution, String sourceChunkId, String tenantId,
                                            boolean acceptInstanceId, ConnectionParams okapiParams) {
    if (CollectionUtils.isEmpty(rawRecords)) {
      return Future.succeededFuture(Collections.emptyList());
    }
    var counter = new MutableInt();
    // if number of records is more than thresholdChunkSize update the progress every 20% of processed records,
    // otherwise update it once after all the records are processed
    int partition = rawRecords.size() > thresholdChunkSize ? rawRecords.size() / 5 : rawRecords.size();
    var records = getParsedRecordsFromInitialRecords(rawRecords, recordContentType, jobExecution, acceptInstanceId,
      sourceChunkId).stream()
      .peek(stat -> { //NOSONAR
        if (counter.incrementAndGet() % partition == 0) {
          log.info("parseRecords:: Parsed {} records out of {}", counter.intValue(), rawRecords.size());
          SourceChunkStateUpdater.update(jobExecutionSourceChunkDao, sourceChunkId, tenantId, "update progress of", jobExecution.getId(),
            chunk -> chunk.withProcessedAmount(chunk.getProcessedAmount() + counter.intValue()));
        }
      }).toList();

    return this.postProcessRecords(jobExecution, records, okapiParams);
  }

  private ParsedResult addErrorMessageWhen999ffFieldExistsOnCreateAction(JobExecution jobExecution,
                                                                         ParsedResult parsedResult) {
    if (jobExecution.getJobProfileInfo().getDataType().equals(DataType.MARC)
        && parsedResult.getParsedRecord() != null) {
      var tmpRecord = new Record()
        .withParsedRecord(new ParsedRecord().withContent(parsedResult.getParsedRecord().encode()));
      if ((StringUtils.isNotBlank(getValue(tmpRecord, TAG_999, SUBFIELD_S, INDICATOR_F, INDICATOR_F))
           || StringUtils.isNotBlank(getValue(tmpRecord, TAG_999, SUBFIELD_I, INDICATOR_F, INDICATOR_F)))) {
        if (hasCreateInstanceActionWithoutPrecedingDelete999Field(jobExecution)) {
          return constructParsedResultWithError(parsedResult, INSTANCE_CREATION_999_ERROR_MESSAGE);
        } else if (hasCreateMarcHoldingsAction(jobExecution)) {
          return constructParsedResultWithError(parsedResult, HOLDINGS_CREATION_999_ERROR_MESSAGE);
        } else if (hasCreateAuthorityAction(jobExecution)) {
          return constructParsedResultWithError(parsedResult, AUTHORITY_CREATION_999_ERROR_MESSAGE);
        }
      }
    }
    return parsedResult;
  }

  private ParsedResult constructParsedResultWithError(ParsedResult parsedResult, String errorMessage) {
    ParsedResult result = new ParsedResult();
    JsonObject errorObject = new JsonObject();
    errorObject.put("error", errorMessage);
    result.setErrors(errorObject);
    result.setParsedRecord(parsedResult.getParsedRecord());
    return result;
  }

  private void postProcessMarcRecord(Record record, InitialRecord rawRecord) {
    var recordType = record.getRecordType();
    if (recordType == MARC_BIB) {
      postProcessMarcBibRecord(record);
    } else if (recordType == MARC_HOLDING) {
      postProcessMarcHoldingsRecord(record, rawRecord);
    }
  }

  private void postProcessMarcBibRecord(Record record) {
    String instanceId = getValue(record, TAG_999, SUBFIELD_I, INDICATOR_F, INDICATOR_F);
    if (isNotBlank(instanceId)) {
      record.setExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));
      String instanceHrid = getControlFieldValue(record, TAG_001);
      if (isNotBlank(instanceHrid)) {
        record.getExternalIdsHolder().setInstanceHrid(instanceHrid);
      }
    }
  }

  private void postProcessMarcHoldingsRecord(Record record, InitialRecord rawRecord) {
    if (isBlank(getControlFieldValue(record, TAG_004))) {
      marcHoldingsVerifier.markMissing004Link(record, rawRecord);
    }
  }

  private Future<List<Record>> postProcessRecords(JobExecution jobExecution, List<Record> folioRecords,
                                                  ConnectionParams okapiParams) {
    if (shouldRemoveSubfield9(jobExecution.getJobProfileSnapshotWrapper())) {
      return fieldModificationService.remove9Subfields(jobExecution.getId(), folioRecords, okapiParams);
    }

    return Future.succeededFuture(folioRecords);
  }

  private RecordType inferRecordType(JobExecution jobExecution, ParsedResult recordParsedResult, String recordId,
                                     String chunkId) {
    if (Objects.equals(jobExecution.getJobProfileInfo().getDataType(), DataType.MARC)) {
      MarcRecordType marcRecordType = marcRecordAnalyzer.process(recordParsedResult.getParsedRecord());
      checkLeaderLine(marcRecordType, recordParsedResult, jobExecution, recordId, chunkId);
      return MarcRecordType.NA == marcRecordType ? null : RecordType.valueOf(MARC_FORMAT + marcRecordType.name());
    }

    return RecordType.valueOf(jobExecution.getJobProfileInfo().getDataType().value());
  }

  private void checkLeaderLine(MarcRecordType marcRecordType, ParsedResult recordParsedResult,
                               JobExecution jobExecution, String recordId, String chunkId) {
    String fileName = StringUtils.defaultIfEmpty(jobExecution.getFileName(), "No file name");
    JsonObject parsedRecord = Objects.requireNonNullElse(recordParsedResult.getParsedRecord(), new JsonObject());
    if (parsedRecord.containsKey("leader") && marcRecordType == MarcRecordType.NA) {
      recordParsedResult.setErrors(new JsonObject()
        .put(MESSAGE_KEY,
          String.format("Error during analyze leader line for determining record type for record with id %s", recordId))
        .put("error", parsedRecord));
      log.warn(
        "checkLeaderLine:: Marc record analyzer found problem on leader line in marc file for record with id: {}, for jobExecutionId: {} from chunk with id: {} from file: {}",
        recordId, jobExecution.getId(), chunkId, fileName);
    } else {
      log.info(
        "checkLeaderLine:: Marc record analyzer parsed record with id: {} and type: {} for jobExecutionId: {} from chunk with id: {} from file: {}",
        recordId, marcRecordType, jobExecution.getId(), chunkId, fileName);
    }
  }

  /**
   * Adds new additional fields into parsed records content to incoming records
   *
   * @param records list of records
   */
  private void fillParsedRecordsWithAdditionalFields(List<Record> records) {
    if (CollectionUtils.isEmpty(records)) {
      return;
    }
    Record.RecordType recordType = records.getFirst().getRecordType();
    if (MARC_BIB.equals(recordType) || MARC_HOLDING.equals(recordType)) {
      records.forEach(this::addMatchedIdToMarcRecord);
    } else if (MARC_AUTHORITY.equals(recordType)) {
      records.stream().filter(record -> record.getParsedRecord() != null).forEach(this::fillAuthorityRecord);
    }
  }

  private void addMatchedIdToMarcRecord(Record record) {
    if (record.getMatchedId() != null) {
      addFieldToMarcRecord(record, TAG_999, SUBFIELD_S, record.getMatchedId());
    }
  }

  private void fillAuthorityRecord(Record record) {
    addMatchedIdToMarcRecord(record);
    final String authorityId = setAuthorityIdIfAbsentForRecord(record);
    Optional.ofNullable(getControlFieldValue(record, TAG_001))
      .map(String::trim)
      .ifPresentOrElse(hrId -> record.setExternalIdsHolder(
          new ExternalIdsHolder().withAuthorityId(authorityId).withAuthorityHrid(hrId)),
        () -> {
          record.setExternalIdsHolder(new ExternalIdsHolder().withAuthorityId(authorityId));
          log.warn("fillParsedRecordsWithAdditionalFields:: record with id: {} does not contain the hrId field",
            record.getId());
        });
  }

  private String setAuthorityIdIfAbsentForRecord(Record record) {
    String authorityId = getValue(record, TAG_999, SUBFIELD_I, INDICATOR_F, INDICATOR_F);
    if (isNotBlank(authorityId)) {
      return authorityId;
    }

    authorityId = UUID.randomUUID().toString();
    addFieldToMarcRecord(record, TAG_999, SUBFIELD_I, authorityId);
    return authorityId;
  }

  /**
   * Saves parsed records in mod-source-record-storage
   *
   * @param params        - okapi params
   * @param jobExecution  - job execution related to records
   * @param parsedRecords - parsed records
   */
  private Future<List<Record>> saveRecords(ConnectionParams params, JobExecution jobExecution,
                                           List<Record> parsedRecords) {
    if (CollectionUtils.isEmpty(parsedRecords)) {
      return Future.succeededFuture();
    }
    log.info("saveRecords:: Saving records in SRS, amount: {}, jobExecutionId: {}", parsedRecords.size(),
      jobExecution.getId());
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(parsedRecords)
      .withTotalRecords(parsedRecords.size());

    List<KafkaHeader> kafkaHeaders = KafkaHeaderUtils.kafkaHeadersFromMap(params.getHeaders());

    kafkaHeaders.add(new KafkaHeaderImpl(JOB_EXECUTION_ID_HEADER, jobExecution.getId()));
    kafkaHeaders.add(new KafkaHeaderImpl(USER_ID_HEADER, jobExecution.getUserId()));

    String key = distributionKeyGenerator.next();

    return sendEventToKafka(params.getTenantId(), Json.encode(recordCollection), DI_RAW_RECORDS_CHUNK_PARSED.value(),
      kafkaHeaders, kafkaConfig, key)
      .map(parsedRecords);
  }

  private String prepareWrongJobProfileErrorMessage(JobExecution jobExecution, List<Record> records) {
    return String.format(WRONG_JOB_PROFILE_ERROR_MESSAGE, jobExecution.getJobProfileInfo().getName(),
      records.getFirst().getRecordType());
  }

  private enum ActionType {
    UPDATE_RECORD, DELETE_RECORD, SEND_ERROR, SEND_MARC_BIB, SEND_EDIFACT, SAVE_RECORD, CREATE_ORDER
  }
}
