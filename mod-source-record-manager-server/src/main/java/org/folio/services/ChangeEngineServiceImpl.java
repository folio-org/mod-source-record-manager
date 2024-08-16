package org.folio.services;

import static java.lang.Boolean.TRUE;
import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.folio.rest.RestVerticle.MODULE_SPECIFIC_ARGS;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INCOMING_EDIFACT_RECORD_PARSED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INCOMING_MARC_BIB_FOR_ORDER_PARSED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_MARC_FOR_DELETE_RECEIVED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_MARC_FOR_UPDATE_RECEIVED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_PARSED;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MATCH_PROFILE;
import static org.folio.rest.jaxrs.model.ReactToType.NON_MATCH;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_HOLDING;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.SUBFIELD_I;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.SUBFIELD_S;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.TAG_999;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.addFieldToMarcRecord;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.getControlFieldValue;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.getValue;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.hasIndicator;
import static org.folio.services.journal.JournalUtil.getJournalMessageProducer;
import static org.folio.services.util.EventHandlingUtil.sendEventToKafka;
import static org.folio.verticle.consumers.StoredRecordChunksKafkaHandler.ACTION_FIELD;
import static org.folio.verticle.consumers.StoredRecordChunksKafkaHandler.CREATE_ACTION;
import static org.folio.verticle.consumers.StoredRecordChunksKafkaHandler.FOLIO_RECORD;
import static org.folio.verticle.consumers.StoredRecordChunksKafkaHandler.ORDER_TYPE;

import com.google.common.collect.Lists;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageProducer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.impl.KafkaHeaderImpl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.ws.rs.NotFoundException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.MappingProfile;
import org.folio.rest.jaxrs.model.IncomingRecord;
import org.folio.services.exceptions.InvalidJobProfileForFileException;
import org.folio.services.journal.BatchableJournalRecord;
import org.folio.services.journal.JournalUtil;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.marc.MarcRecordAnalyzer;
import org.folio.dataimport.util.marc.MarcRecordType;
import org.folio.dataimport.util.marc.RecordAnalyzer;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.rest.client.SourceStorageBatchClient;
import org.folio.rest.jaxrs.model.ActionProfile;
import org.folio.rest.jaxrs.model.ActionProfile.Action;
import org.folio.rest.jaxrs.model.ActionProfile.FolioRecord;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.JobProfileInfo.DataType;
import org.folio.rest.jaxrs.model.MatchProfile;
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
import org.folio.services.parsers.ParsedResult;
import org.folio.services.parsers.RecordParserBuilder;
import org.folio.services.util.RecordConversionUtil;
import org.folio.services.validation.JobProfileSnapshotValidationService;
import org.folio.verticle.consumers.util.JobExecutionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ChangeEngineServiceImpl implements ChangeEngineService {

  public static final String JOB_EXECUTION_ID_HEADER = "jobExecutionId";
  public static final String RECORD_ID_HEADER = "recordId";
  public static final String USER_ID_HEADER = "userId";

  private static final String MESSAGE_KEY = "message";
  private static final Logger LOGGER = LogManager.getLogger();
  private static final int THRESHOLD_CHUNK_SIZE =
    Integer.parseInt(MODULE_SPECIFIC_ARGS.getOrDefault("chunk.processing.threshold.chunk.size", "100"));
  private static final String TAG_001 = "001";
  private static final String TAG_004 = "004";
  private static final String MARC_FORMAT = "MARC_";
  private static final AtomicInteger indexer = new AtomicInteger();
  private static final String HOLDINGS_004_TAG_ERROR_MESSAGE =
    "The 004 tag of the Holdings doesn't has a link to the Bibliographic record";
  private static final String INSTANCE_CREATION_999_ERROR_MESSAGE = "A new Instance was not created because the incoming record already contained a 999ff$s or 999ff$i field";
  private static final String HOLDINGS_CREATION_999_ERROR_MESSAGE = "A new MARC-Holding was not created because the incoming record already contained a 999ff$s or 999ff$i field";
  private static final String AUTHORITY_CREATION_999_ERROR_MESSAGE = "A new MARC-Authority was not created because the incoming record already contained a 999ff$s or 999ff$i field";
  private static final String WRONG_JOB_PROFILE_ERROR_MESSAGE = "Chosen job profile '%s' does not support '%s' record type";
  private static final String JOB_PROFILE_HAS_NO_CHILD_PROFILES_ERROR_MESSAGE = "The '%s' job profile does not have any linked action or matching profiles";
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
  private MessageProducer<Collection<BatchableJournalRecord>> journalRecordProducer;

  @Value("${srm.kafka.RawChunksKafkaHandler.maxDistributionNum:100}")
  private int maxDistributionNum;

  @Value("${marc.holdings.batch.size:100}")
  private int batchSize;

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
    this.journalRecordProducer = getJournalMessageProducer(vertx);
  }

  @Override
  public Future<List<Record>> parseRawRecordsChunkForJobExecution(RawRecordsDto chunk, JobExecution jobExecution,
                                                                  String sourceChunkId, boolean acceptInstanceId, OkapiConnectionParams params) {
    Promise<List<Record>> promise = Promise.promise();
    Future<List<Record>> futureParsedRecords =
      parseRecords(chunk.getInitialRecords(), chunk.getRecordsMetadata().getContentType(), jobExecution, sourceChunkId,
        params.getTenantId(), acceptInstanceId, params);

    futureParsedRecords
      .compose(parsedRecords -> {
        saveIncomingAndJournalRecords(parsedRecords, params.getTenantId());
        return filterParsedRecords(jobExecution, params, parsedRecords);
      })
      .compose(filteredParsedRecords -> validateJobProfile(jobExecution, filteredParsedRecords).map(filteredParsedRecords))
      .compose(parsedRecords -> ensureMappingMetaDataSnapshot(jobExecution.getId(), parsedRecords, params)
        .map(parsedRecords))
      .onSuccess(parsedRecords -> {
        fillParsedRecordsWithAdditionalFields(parsedRecords);
        processRecords(parsedRecords, jobExecution, params, sourceChunkId, acceptInstanceId, promise);
      }).onFailure(th -> {
        LOGGER.warn("parseRawRecordsChunkForJobExecution:: Error parsing records, cause: {}, jobExecutionId: {}",
          th.getMessage(), jobExecution.getId());
        promise.fail(th);
      });
    return promise.future();
  }

  private Future<List<Record>> filterParsedRecords(JobExecution jobExecution, OkapiConnectionParams params, List<Record> parsedRecords) {
    Promise<List<Record>> promiseFilteredRecords = Promise.promise();

    List<Future<List<String>>> listFuture = executeInBatches(parsedRecords, batch -> verifyMarcHoldings004Field(batch, params));
    filterMarcHoldingsBy004Field(parsedRecords, listFuture, params, jobExecution, promiseFilteredRecords);
    return promiseFilteredRecords.future();
  }

  private Future<Void> validateJobProfile(JobExecution jobExecution, List<Record> records) {
    ProfileSnapshotWrapper jobProfileSnapshot = jobExecution.getJobProfileSnapshotWrapper();
    if (CollectionUtils.isEmpty(jobProfileSnapshot.getChildSnapshotWrappers())) {
      return Future.failedFuture(new InvalidJobProfileForFileException(
        records, String.format(JOB_PROFILE_HAS_NO_CHILD_PROFILES_ERROR_MESSAGE, jobExecution.getJobProfileInfo().getName())));
    }

    return isJobProfileCompatibleWithRecordsType(jobProfileSnapshot, records)
      ? Future.succeededFuture()
      : Future.failedFuture(new InvalidJobProfileForFileException(records,
      prepareWrongJobProfileErrorMessage(jobExecution, records)));
  }

  private void processRecords(List<Record> parsedRecords, JobExecution jobExecution, OkapiConnectionParams params,
                              String sourceChunkId, boolean acceptInstanceId, Promise<List<Record>> promise) {
    switch (getAction(parsedRecords, jobExecution)) {
      case UPDATE_RECORD -> {
        hrIdFieldService.move001valueTo035Field(parsedRecords);
        updateRecords(parsedRecords, jobExecution, params)
          .onSuccess(ar -> promise.complete(parsedRecords)).onFailure(promise::fail);
      }
      case DELETE_RECORD -> deleteRecords(parsedRecords, jobExecution, params)
        .onSuccess(ar -> promise.complete(parsedRecords)).onFailure(promise::fail);
      case CREATE_ORDER -> sendEvents(parsedRecords, jobExecution, params, DI_INCOMING_MARC_BIB_FOR_ORDER_PARSED)
        .onSuccess(ar -> promise.complete(parsedRecords)).onFailure(promise::fail);
      case SEND_ERROR -> sendEvents(parsedRecords, jobExecution, params, DI_ERROR)
        .onSuccess(ar -> promise.complete(parsedRecords)).onFailure(promise::fail);
      case SEND_MARC_BIB -> sendEventWithContext(parsedRecords, jobExecution, params, Map.of(ACCEPT_INSTANCE_ID_KEY, Boolean.toString(acceptInstanceId)))
        .onSuccess(ar -> promise.complete(parsedRecords)).onFailure(promise::fail);
      case SEND_EDIFACT -> sendEvents(parsedRecords, jobExecution, params, DI_INCOMING_EDIFACT_RECORD_PARSED)
        .onSuccess(ar -> promise.complete(parsedRecords)).onFailure(promise::fail);
      default -> saveRecords(jobExecution, sourceChunkId, params, parsedRecords, promise);
    }
  }

  private ActionType getAction(List<Record> parsedRecords, JobExecution jobExecution) {
    if (updateMarcActionExists(jobExecution) || updateInstanceActionExists(jobExecution)
      || isCreateOrUpdateItemOrHoldingsActionExists(jobExecution, parsedRecords) || isMarcAuthorityMatchProfile(jobExecution)) {
      return ActionType.UPDATE_RECORD;
    }
    if (deleteMarcActionExists(jobExecution)) {
      return ActionType.DELETE_RECORD;
    }
    if (createOrderActionExists(jobExecution)) {
      return ActionType.CREATE_ORDER;
    }
    if (parsedRecords.isEmpty()) {
      return ActionType.SAVE_RECORD;
    }
    RecordType recordType = parsedRecords.get(0).getRecordType();
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

  private enum ActionType {
    UPDATE_RECORD, DELETE_RECORD, SEND_ERROR, SEND_MARC_BIB, SEND_EDIFACT, SAVE_RECORD, CREATE_ORDER
  }

  private void saveRecords(JobExecution jobExecution, String sourceChunkId, OkapiConnectionParams params, List<Record> parsedRecords, Promise<List<Record>> promise) {
    saveRecords(params, jobExecution, parsedRecords)
      .onComplete(postAr -> {
        if (postAr.failed()) {
          StatusDto statusDto = new StatusDto()
            .withStatus(StatusDto.Status.ERROR)
            .withErrorStatus(StatusDto.ErrorStatus.RECORD_UPDATE_ERROR);
          jobExecutionService.updateJobExecutionStatus(jobExecution.getId(), statusDto, params)
            .onComplete(r -> {
              if (r.failed()) {
                LOGGER.warn("parseRawRecordsChunkForJobExecution:: Error during update jobExecution with id '{}' and snapshot status",
                  jobExecution.getId(), r.cause());
              }
            });
          jobExecutionSourceChunkDao.getById(sourceChunkId, params.getTenantId())
            .compose(optional -> optional
              .map(sourceChunk -> jobExecutionSourceChunkDao
                .update(sourceChunk.withState(JobExecutionSourceChunk.State.ERROR), params.getTenantId()))
              .orElseThrow(() -> new NotFoundException(String.format(
                "Couldn't update failed jobExecutionSourceChunk status to ERROR, jobExecutionSourceChunk with id %s was not found, jobExecutionId: %s",
                sourceChunkId, jobExecution.getId()))))
            .onComplete(ar -> promise.fail(postAr.cause()));
        } else {
          promise.complete(parsedRecords);
        }
      });
  }

  private Future<Boolean> sendEvents(List<Record> records, JobExecution jobExecution, OkapiConnectionParams params, DataImportEventTypes eventType) {
    LOGGER.info("sendEvents:: Sending events with type: {}, jobExecutionId: {}", eventType.value(), jobExecution.getId());
    return recordsPublishingService.sendEventsWithRecords(records, jobExecution.getId(), params, eventType.value(), null);
  }

  private Future<Boolean> sendEventWithContext(List<Record> records, JobExecution jobExecution, OkapiConnectionParams params, Map<String, String> eventContext) {
    LOGGER.info("sendEvents:: Sending events with type: {}, jobExecutionId: {}, event context: {}", DI_INCOMING_MARC_BIB_RECORD_PARSED, jobExecution.getId(), eventContext);
    return recordsPublishingService.sendEventsWithRecords(records, jobExecution.getId(), params, DI_INCOMING_MARC_BIB_RECORD_PARSED.value(), eventContext);
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

  private boolean createOrderActionExists(JobExecution jobExecution) {
    if (jobExecution.getJobProfileSnapshotWrapper() != null) {
      List<ProfileSnapshotWrapper> actionProfiles = jobExecution.getJobProfileSnapshotWrapper().getChildSnapshotWrappers()
        .stream().filter(wrapper -> wrapper.getContentType() == ACTION_PROFILE).toList();

      if (!actionProfiles.isEmpty() && ifOrderCreateActionProfileExists(actionProfiles)) {
        LOGGER.debug("createOrderActionExists:: Event type for Order's logic set by jobExecutionId {} ", jobExecution.getId());
        return true;
      }
    }
    return false;
  }

  private static boolean ifOrderCreateActionProfileExists(List<ProfileSnapshotWrapper> profiles) {
    for (ProfileSnapshotWrapper profile : profiles) {
      Map<String, String> content = DatabindCodec.mapper().convertValue(profile.getContent(), HashMap.class);
      if (content.get(FOLIO_RECORD).equals(ORDER_TYPE) && content.get(ACTION_FIELD).equals(CREATE_ACTION)) {
        return true;
      }
    }
    return false;
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
  private boolean isJobProfileCompatibleWithRecordsType(ProfileSnapshotWrapper jobProfileSnapshot, List<Record> records) {
    if (records.isEmpty()) {
      return true;
    }
    RecordType recordType = records.get(0).getRecordType();
    return recordType == null || jobProfileSnapshotValidationService.isJobProfileCompatibleWithRecordType(jobProfileSnapshot, recordType);
  }

  private Future<Boolean> updateRecords(List<Record> records, JobExecution jobExecution, OkapiConnectionParams params) {
    LOGGER.info("updateRecords:: Records have not been saved in record-storage, because job contains action for Marc or Instance update");
    return recordsPublishingService
      .sendEventsWithRecords(records, jobExecution.getId(), params, DI_MARC_FOR_UPDATE_RECEIVED.value(), null);
  }

  private Future<Boolean> deleteRecords(List<Record> records, JobExecution jobExecution, OkapiConnectionParams params) {
    LOGGER.info("deleteRecords:: Records have not been saved in record-storage, because job contains action for Marc delete");
    return recordsPublishingService
      .sendEventsWithRecords(records, jobExecution.getId(), params, DI_MARC_FOR_DELETE_RECEIVED.value(), null);
  }

  private Future<Boolean> ensureMappingMetaDataSnapshot(String jobExecutionId, List<Record> recordsList,
                                                        OkapiConnectionParams okapiParams) {
    if (CollectionUtils.isEmpty(recordsList)) {
      return Future.succeededFuture(false);
    }
    Promise<Boolean> promise = Promise.promise();
    mappingMetadataService.getMappingMetadataDto(jobExecutionId, okapiParams)
      .onSuccess(v -> promise.complete(false))
      .onFailure(e -> {
        if (e instanceof NotFoundException) {
          RecordType recordType = recordsList.get(0).getRecordType();
          recordType = Objects.isNull(recordType) || recordType == RecordType.EDIFACT ? MARC_BIB : recordType;
          mappingMetadataService.saveMappingRulesSnapshot(jobExecutionId, recordType.toString(), okapiParams.getTenantId())
            .compose(arMappingRules -> mappingMetadataService.saveMappingParametersSnapshot(jobExecutionId, okapiParams))
            .onSuccess(ar -> promise.complete(true))
            .onFailure(promise::fail);
          return;
        }
        promise.fail(e);
      });
    return promise.future();
  }

  private boolean updateMarcActionExists(JobExecution jobExecution) {
    return containsMarcActionProfile(
      jobExecution.getJobProfileSnapshotWrapper(),
      List.of(FolioRecord.MARC_BIBLIOGRAPHIC, FolioRecord.MARC_AUTHORITY, FolioRecord.MARC_HOLDINGS),
      Action.UPDATE);
  }

  private boolean updateInstanceActionExists(JobExecution jobExecution) {
    return containsMarcActionProfile(
      jobExecution.getJobProfileSnapshotWrapper(),
      List.of(FolioRecord.INSTANCE),
      Action.UPDATE);
  }

  private boolean isCreateOrUpdateItemOrHoldingsActionExists(JobExecution jobExecution, List<Record> parsedRecords) {
    var jobProfileSnapshotWrapper = jobExecution.getJobProfileSnapshotWrapper();
    var itemAndHoldingsList = List.of(FolioRecord.ITEM, FolioRecord.HOLDINGS);
    return (containsMarcActionProfile(jobProfileSnapshotWrapper, itemAndHoldingsList, Action.CREATE) ||
      containsMarcActionProfile(jobProfileSnapshotWrapper, itemAndHoldingsList, Action.UPDATE)) &&
      !containsMarcActionProfile(jobProfileSnapshotWrapper, List.of(FolioRecord.INSTANCE), Action.CREATE) &&
      !CollectionUtils.isEmpty(parsedRecords) &&
      parsedRecords.get(0).getRecordType() == MARC_BIB;
  }

  private boolean deleteMarcActionExists(JobExecution jobExecution) {
    return containsMarcActionProfile(
      jobExecution.getJobProfileSnapshotWrapper(),
      List.of(FolioRecord.MARC_AUTHORITY),
      Action.DELETE);
  }

  private boolean isCreateInstanceActionExists(JobExecution jobExecution) {
    return containsCreateInstanceActionWithoutMarcBib(jobExecution.getJobProfileSnapshotWrapper());
  }

  private boolean containsCreateInstanceActionWithoutMarcBib(ProfileSnapshotWrapper profileSnapshot) {
    for (ProfileSnapshotWrapper childWrapper : profileSnapshot.getChildSnapshotWrappers()) {
      if (childWrapper.getContentType() == ACTION_PROFILE
        && actionProfileMatches(childWrapper, List.of(FolioRecord.INSTANCE), Action.CREATE)) {
        return childWrapper.getReactTo() != NON_MATCH && !containsMarcBibToInstanceMappingProfile(childWrapper);
      } else if (containsCreateInstanceActionWithoutMarcBib(childWrapper)) {
        return true;
      }
    }
    return false;
  }

  private boolean containsMarcBibToInstanceMappingProfile(ProfileSnapshotWrapper actionWrapper) {
   return actionWrapper.getChildSnapshotWrappers()
      .stream()
      .map(mappingWrapper -> Optional.ofNullable(mappingWrapper.getContent()))
      .filter(Optional::isPresent)
      .map(content -> DatabindCodec.mapper().convertValue(content.get(), MappingProfile.class))
      .anyMatch(mappingProfile -> mappingProfile.getIncomingRecordType() == EntityType.MARC_BIBLIOGRAPHIC);
  }

  private boolean isCreateAuthorityActionExists(JobExecution jobExecution) {
    return containsMarcActionProfile(
      jobExecution.getJobProfileSnapshotWrapper(),
      List.of(FolioRecord.AUTHORITY),
      Action.CREATE);
  }

  private boolean isCreateMarcHoldingsActionExists(JobExecution jobExecution) {
    return containsCreateActionProfileWithMarcHoldings(
      jobExecution.getJobProfileSnapshotWrapper());
  }

  private boolean isMarcAuthorityMatchProfile(JobExecution jobExecution) {
    return containsMatchProfile(jobExecution.getJobProfileSnapshotWrapper());
  }

  private boolean containsMatchProfile(ProfileSnapshotWrapper profileSnapshot) {
    var childWrappers = profileSnapshot.getChildSnapshotWrappers();
    for (ProfileSnapshotWrapper childWrapper : childWrappers) {
      if (childWrapper.getContentType() == MATCH_PROFILE
        && marcAuthorityMatchProfileMatches(childWrapper)) {
        return true;
      } else if (containsMatchProfile(childWrapper)) {
        return true;
      }
    }
    return false;
  }

  private boolean marcAuthorityMatchProfileMatches(ProfileSnapshotWrapper matchProfileWrapper) {
    MatchProfile matchProfile = new JsonObject((Map) matchProfileWrapper.getContent()).mapTo(MatchProfile.class);
    return matchProfile.getExistingRecordType() == EntityType.MARC_AUTHORITY && matchProfile.getIncomingRecordType() == EntityType.MARC_AUTHORITY;
  }

  private boolean containsMarcActionProfile(ProfileSnapshotWrapper profileSnapshot,
                                            List<FolioRecord> entityTypes, Action action) {
    List<ProfileSnapshotWrapper> childWrappers = profileSnapshot.getChildSnapshotWrappers();
    for (ProfileSnapshotWrapper childWrapper : childWrappers) {
      if (childWrapper.getContentType() == ACTION_PROFILE
        && actionProfileMatches(childWrapper, entityTypes, action)) {
        return true;
      } else if (containsMarcActionProfile(childWrapper, entityTypes, action)) {
        return true;
      }
    }
    return false;
  }

  private boolean containsCreateActionProfileWithMarcHoldings(ProfileSnapshotWrapper profileSnapshot) {
    List<ProfileSnapshotWrapper> childWrappers = profileSnapshot.getChildSnapshotWrappers();
    for (ProfileSnapshotWrapper childWrapper : childWrappers) {
      if (childWrapper.getContentType() == ACTION_PROFILE
        && actionProfileMatches(childWrapper, List.of(FolioRecord.HOLDINGS), Action.CREATE)
        && isMarcHoldingsExists(childWrapper)) {
        return true;
      } else if (containsCreateActionProfileWithMarcHoldings(childWrapper)) {
        return true;
      }
    }
    return false;
  }

  private boolean actionProfileMatches(ProfileSnapshotWrapper actionProfileWrapper,
                                       List<FolioRecord> records, Action action) {
    ActionProfile actionProfile = new JsonObject((Map) actionProfileWrapper.getContent()).mapTo(ActionProfile.class);
    return (records.contains(actionProfile.getFolioRecord())) && actionProfile.getAction() == action;
  }

  private boolean isMarcHoldingsExists(ProfileSnapshotWrapper actionProfileWrapper) {
    List<ProfileSnapshotWrapper> childWrappers = actionProfileWrapper.getChildSnapshotWrappers();
    if (childWrappers != null && !childWrappers.isEmpty() && childWrappers.get(0) != null) {
      MappingProfile mappingProfile = new JsonObject((Map) childWrappers.get(0).getContent()).mapTo(MappingProfile.class);
      return mappingProfile.getExistingRecordType() == EntityType.HOLDINGS && mappingProfile.getIncomingRecordType() == EntityType.MARC_HOLDINGS;
    }
    return false;
  }

  /**
   * Parse list of source records
   *
   * @param rawRecords       - list of raw records for parsing
   * @param jobExecution     - job execution of record's parsing
   * @param sourceChunkId    - id of the JobExecutionSourceChunk
   * @param tenantId         - tenant id
   * @param acceptInstanceId - allow the 999ff$i field to be set and also create an instance with value in 999ff$i
   * @param okapiParams      - OkapiConnectionParams to interact with external services
   * @return - list of records with parsed or error data
   */
  private Future<List<Record>> parseRecords(List<InitialRecord> rawRecords, RecordsMetadata.ContentType recordContentType,
                                            JobExecution jobExecution, String sourceChunkId, String tenantId,
                                            boolean acceptInstanceId, OkapiConnectionParams okapiParams) {
    if (CollectionUtils.isEmpty(rawRecords)) {
      return Future.succeededFuture(Collections.emptyList());
    }
    var counter = new MutableInt();
    // if number of records is more than THRESHOLD_CHUNK_SIZE update the progress every 20% of processed records,
    // otherwise update it once after all the records are processed
    int partition = rawRecords.size() > THRESHOLD_CHUNK_SIZE ? rawRecords.size() / 5 : rawRecords.size();
    var records = getParsedRecordsFromInitialRecords(rawRecords, recordContentType, jobExecution, acceptInstanceId, sourceChunkId).stream()
      .peek(stat -> { //NOSONAR
        if (counter.incrementAndGet() % partition == 0) {
          LOGGER.info("parseRecords:: Parsed {} records out of {}", counter.intValue(), rawRecords.size());
          jobExecutionSourceChunkDao.getById(sourceChunkId, tenantId)
            .compose(optional -> optional
              .map(sourceChunk -> jobExecutionSourceChunkDao
                .update(sourceChunk.withProcessedAmount(sourceChunk.getProcessedAmount() + counter.intValue()), tenantId))
              .orElseThrow(() -> new NotFoundException(format(
                "Couldn't update jobExecutionSourceChunk progress, jobExecutionSourceChunk with id %s was not found",
                sourceChunkId))));
        }
      }).toList();

    return this.postProcessRecords(jobExecution, records, okapiParams);
  }

  public List<Record> getParsedRecordsFromInitialRecords(List<InitialRecord> rawRecords,
                                                         RecordsMetadata.ContentType recordContentType,
                                                         JobExecution jobExecution,
                                                         boolean acceptInstanceId,
                                                         String sourceChunkId) {
    var parser = RecordParserBuilder.buildParser(recordContentType);

    return rawRecords.stream()
      .map(rawRecord -> {
        var parsedResult = parser.parseRecord(rawRecord.getRecord());

        if (!acceptInstanceId) {
          parsedResult = addErrorMessageWhen999ffFieldExistsOnCreateAction(jobExecution, parsedResult);
        } else {
          LOGGER.debug("getParsedRecordsFromInitialRecords:: acceptInstanceId = true, sourceChunkId = {}, jobExecutionId = {} ",
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
          record.setErrorRecord(new ErrorRecord()
            .withContent(rawRecord)
            .withDescription(parsedResult.getErrors().encode()));
        } else {
          record.setParsedRecord(new ParsedRecord().withId(recordId).withContent(parsedResult.getParsedRecord().encode()));
          if (jobExecution.getJobProfileInfo().getDataType().equals(DataType.MARC)) {
            postProcessMarcRecord(record, rawRecord);
          }
        }
        return record;
      }).collect(Collectors.toList());
  }

  private ParsedResult addErrorMessageWhen999ffFieldExistsOnCreateAction(JobExecution jobExecution, ParsedResult parsedResult) {
    if (jobExecution.getJobProfileInfo().getDataType().equals(DataType.MARC) && parsedResult.getParsedRecord() != null) {
      var tmpRecord = new Record()
        .withParsedRecord(new ParsedRecord().withContent(parsedResult.getParsedRecord().encode()));
      if (((StringUtils.isNotBlank(getValue(tmpRecord, TAG_999, SUBFIELD_S)) && hasIndicator(tmpRecord, SUBFIELD_S))
        || (StringUtils.isNotBlank(getValue(tmpRecord, TAG_999, SUBFIELD_I)) && hasIndicator(tmpRecord, SUBFIELD_I)))) {
        if (isCreateInstanceActionExists(jobExecution)) {
          return constructParsedResultWithError(parsedResult, INSTANCE_CREATION_999_ERROR_MESSAGE);
        } else if (isCreateMarcHoldingsActionExists(jobExecution)) {
          return constructParsedResultWithError(parsedResult, HOLDINGS_CREATION_999_ERROR_MESSAGE);
        } else if (isCreateAuthorityActionExists(jobExecution)) {
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

  private List<Future<List<String>>> executeInBatches(List<Record> recordList,
                                        Function<List<String>, Future<List<String>>> batchOperation) {
    // filter list on MARC_HOLDINGS
    var marcHoldingsIdsToVerify = recordList.stream()
      .filter(recordItem -> recordItem.getRecordType() == MARC_HOLDING)
      .map(recordItem -> getControlFieldValue(recordItem, TAG_004))
      .filter(StringUtils::isNotBlank)
      .collect(Collectors.toList());
    // split on batches and create list of Futures
    List<List<String>> batches = Lists.partition(marcHoldingsIdsToVerify, batchSize);
    List<Future<List<String>>> futureList = new ArrayList<>();
    for (List<String> batch : batches) {
      futureList.add(batchOperation.apply(batch));
    }
    return futureList;
  }

  private void filterMarcHoldingsBy004Field(List<Record> records, List<Future<List<String>>> batchList, OkapiConnectionParams okapiParams,
                                            JobExecution jobExecution, Promise<List<Record>> promise) {
    Future.all(batchList)
      .onComplete(as -> {
        if (IterableUtils.matchesAll(records, record -> record.getRecordType() == MARC_HOLDING)) {
          var invalidMarcBibIds = batchList
            .stream()
            .map(Future<List<String>>::result)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
          LOGGER.info("filterMarcHoldingsBy004Field:: MARC_BIB invalid list ids: {}", invalidMarcBibIds);
          var validMarcBibRecords = records.stream()
            .filter(record -> {
              var controlFieldValue = getControlFieldValue(record, TAG_004);
              return isValidMarcHoldings(jobExecution, okapiParams, invalidMarcBibIds, record, controlFieldValue);
            }).collect(Collectors.toList());
          LOGGER.info("filterMarcHoldingsBy004Field:: Total marc holdings records: {}, invalid marc bib ids: {}, valid marc bib records: {}",
            records.size(), invalidMarcBibIds.size(), validMarcBibRecords.size());
          promise.complete(validMarcBibRecords);
        } else {
          promise.complete(records);
        }
      });
  }

  private Future<List<String>> verifyMarcHoldings004Field(List<String> marcBibIds, OkapiConnectionParams okapiParams) {
    Promise<List<String>> promise = Promise.promise();
    var sourceStorageBatchClient = getSourceStorageBatchClient(okapiParams);
    try {
      sourceStorageBatchClient.postSourceStorageBatchVerifiedRecords(marcBibIds, asyncResult -> {
        LOGGER.info("verifyMarcHoldings004Field:: Verify list of marc bib ids: {} ", marcBibIds);
        List<String> invalidMarcBibIds = new ArrayList<>();
        if (asyncResult.succeeded() && asyncResult.result().statusCode() == 200) {
          var body = asyncResult.result().body();
          LOGGER.info("verifyMarcHoldings004Field:: Response from SRS with invalid MARC Bib ids: {}", body);
          var object = new JsonObject(body);
          var ids = object.getJsonArray("invalidMarcBibIds");
          invalidMarcBibIds = ids.getList();
          LOGGER.info("verifyMarcHoldings004Field:: List of marc bib ids: {}", invalidMarcBibIds);
        } else {
          LOGGER.info("verifyMarcHoldings004Field:: The marc holdings not found in the SRS: {} and status code: {}", asyncResult.result(),
            asyncResult.result().statusCode());
        }
        promise.complete(invalidMarcBibIds);
      });
    } catch (Exception e) {
      LOGGER.warn("verifyMarcHoldings004Field:: Error during call post request to SRS: {}", e.getMessage());
      promise.complete(Collections.emptyList());
    }
    return promise.future();
  }

  private boolean isValidMarcHoldings(JobExecution jobExecution, OkapiConnectionParams okapiParams,
                                      List<String> invalidMarcBibIds, Record record, String controlFieldValue) {
    if (isBlank(controlFieldValue) || invalidMarcBibIds.contains(controlFieldValue)) {
      // avoid populating error if there is already populated via 999ff-field error.
      if (record.getErrorRecord() != null && record.getErrorRecord().getDescription().contains("999ff")) {
        return true;
      }
      populateError(record, jobExecution, okapiParams);
      return false;
    }
    return true;
  }

  private void populateError(Record record, JobExecution jobExecution, OkapiConnectionParams okapiParams) {
    var eventPayload = getDataImportPayload(record, jobExecution, okapiParams);
    eventPayload.getContext().put(RECORD_ID_HEADER, record.getId());
    var key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);
    LOGGER.warn(HOLDINGS_004_TAG_ERROR_MESSAGE);
    record.setParsedRecord(null);
    record.setErrorRecord(new ErrorRecord()
      .withContent(record.getRawRecord().getContent())
      .withDescription(new JsonObject().put(MESSAGE_KEY, HOLDINGS_004_TAG_ERROR_MESSAGE).encode())
    );
    var kafkaHeaders = KafkaHeaderUtils.kafkaHeadersFromMultiMap(okapiParams.getHeaders());
    kafkaHeaders.add(new KafkaHeaderImpl(RECORD_ID_HEADER, record.getId()));

    sendEventToKafka(okapiParams.getTenantId(), Json.encode(eventPayload), DI_ERROR.value(), kafkaHeaders, kafkaConfig, key)
      .onFailure(
        th -> LOGGER.warn("populateError:: Error publishing DI_ERROR event for MARC Holdings record with id {}", record.getId(), th));
  }

  private DataImportEventPayload getDataImportPayload(Record record, JobExecution jobExecution,
                                                      OkapiConnectionParams okapiParams) {
    EntityType sourceRecordKey = RecordConversionUtil.getEntityType(record);
    return new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withProfileSnapshot(jobExecution.getJobProfileSnapshotWrapper())
      .withJobExecutionId(record.getSnapshotId())
      .withOkapiUrl(okapiParams.getOkapiUrl())
      .withTenant(okapiParams.getTenantId())
      .withToken(okapiParams.getToken())
      .withContext(new HashMap<>() {{
        put(sourceRecordKey.value(), Json.encode(record));
        put("ERROR", HOLDINGS_004_TAG_ERROR_MESSAGE);
      }});
  }

  private SourceStorageBatchClient getSourceStorageBatchClient(OkapiConnectionParams okapiParams) {
    var token = okapiParams.getToken();
    var okapiUrl = okapiParams.getOkapiUrl();
    var tenantId = okapiParams.getTenantId();
    return new SourceStorageBatchClient(okapiUrl, tenantId, token);
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
    String instanceId = getValue(record, TAG_999, SUBFIELD_I);
    if (isNotBlank(instanceId) && hasIndicator(record, SUBFIELD_I)) {
      record.setExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));
      String instanceHrid = getControlFieldValue(record, TAG_001);
      if (isNotBlank(instanceHrid)) {
        record.getExternalIdsHolder().setInstanceHrid(instanceHrid);
      }
    }
  }

  private void postProcessMarcHoldingsRecord(Record record, InitialRecord rawRecord) {
    if (isBlank(getControlFieldValue(record, TAG_004))) {
      LOGGER.warn(HOLDINGS_004_TAG_ERROR_MESSAGE);
      record.setParsedRecord(null);
      record.setErrorRecord(new ErrorRecord()
        .withContent(rawRecord)
        .withDescription(new JsonObject().put(MESSAGE_KEY, HOLDINGS_004_TAG_ERROR_MESSAGE).encode())
      );
    }
  }

  private Future<List<Record>> postProcessRecords(JobExecution jobExecution, List<Record> folioRecords,
                                                  OkapiConnectionParams okapiParams) {
    if (TRUE.equals(shouldRemoveSubfield9FromRecordFieldsForProfile(jobExecution.getJobProfileSnapshotWrapper()))) {
      return fieldModificationService.remove9Subfields(jobExecution.getId(), folioRecords, okapiParams);
    }

    return Future.succeededFuture(folioRecords);
  }

  private Boolean shouldRemoveSubfield9FromRecordFieldsForProfile(ProfileSnapshotWrapper profileSnapshot) {
    for (ProfileSnapshotWrapper childWrapper : profileSnapshot.getChildSnapshotWrappers()) {
      if (childWrapper.getContentType() == ACTION_PROFILE) {
        ActionProfile actionProfile = DatabindCodec.mapper().convertValue(childWrapper.getContent(), ActionProfile.class);
        if (TRUE.equals(actionProfile.getRemove9Subfields())
          || TRUE.equals(shouldRemoveSubfield9FromRecordFieldsForProfile(childWrapper))) {
          return true;
        }
      } else if (TRUE.equals(shouldRemoveSubfield9FromRecordFieldsForProfile(childWrapper))) {
        return true;
      }
    }
    return false;
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

  private void checkLeaderLine(MarcRecordType marcRecordType, ParsedResult recordParsedResult, JobExecution jobExecution, String recordId, String chunkId) {
    String fileName = StringUtils.defaultIfEmpty(jobExecution.getFileName(), "No file name");
    JsonObject parsedRecord = Objects.requireNonNullElse(recordParsedResult.getParsedRecord(), new JsonObject());
    if (parsedRecord.containsKey("leader") && marcRecordType == MarcRecordType.NA) {
      recordParsedResult.setErrors(new JsonObject()
        .put(MESSAGE_KEY, String.format("Error during analyze leader line for determining record type for record with id %s", recordId))
        .put("error", parsedRecord));
      LOGGER.warn("checkLeaderLine:: Marc record analyzer found problem on leader line in marc file for record with id: {}, for jobExecutionId: {} from chunk with id: {} from file: {}",
        recordId, jobExecution.getId(), chunkId, fileName);
    } else {
      LOGGER.info("checkLeaderLine:: Marc record analyzer parsed record with id: {} and type: {} for jobExecutionId: {} from chunk with id: {} from file: {}",
        recordId, marcRecordType, jobExecution.getId(), chunkId, fileName);
    }
  }

  /**
   * Adds new additional fields into parsed records content to incoming records
   *
   * @param records list of records
   */
  private void fillParsedRecordsWithAdditionalFields(List<Record> records) {
    if (!CollectionUtils.isEmpty(records)) {
      Record.RecordType recordType = records.get(0).getRecordType();
      if (MARC_BIB.equals(recordType) || MARC_HOLDING.equals(recordType)) {
        for (Record record : records) {
          if (record.getMatchedId() != null) {
            addFieldToMarcRecord(record, TAG_999, SUBFIELD_S, record.getMatchedId());
          }
        }
      } else if (MARC_AUTHORITY.equals(recordType)) {
        for (Record record : records) {
          if (record.getParsedRecord() != null) {
            if (record.getMatchedId() != null) {
              addFieldToMarcRecord(record, TAG_999, SUBFIELD_S, record.getMatchedId());
            }
            String inventoryId = UUID.randomUUID().toString();
            addFieldToMarcRecord(record, TAG_999, SUBFIELD_I, inventoryId);
            var hrid = getControlFieldValue(record, TAG_001).trim();
            record.setExternalIdsHolder(new ExternalIdsHolder().withAuthorityId(inventoryId).withAuthorityHrid(hrid));
          }
        }
      }
    }
  }

  /**
   * Saves parsed records in mod-source-record-storage
   *
   * @param params        - okapi params
   * @param jobExecution  - job execution related to records
   * @param parsedRecords - parsed records
   */
  private Future<List<Record>> saveRecords(OkapiConnectionParams params, JobExecution jobExecution,
                                           List<Record> parsedRecords) {
    if (CollectionUtils.isEmpty(parsedRecords)) {
      return Future.succeededFuture();
    }
    LOGGER.info("saveRecords:: Saving records in SRS, amount: {}, jobExecutionId: {}", parsedRecords.size(), jobExecution.getId());
    RecordCollection recordCollection = new RecordCollection()
      .withRecords(parsedRecords)
      .withTotalRecords(parsedRecords.size());

    List<KafkaHeader> kafkaHeaders = KafkaHeaderUtils.kafkaHeadersFromMultiMap(params.getHeaders());

    kafkaHeaders.add(new KafkaHeaderImpl(JOB_EXECUTION_ID_HEADER, jobExecution.getId()));
    kafkaHeaders.add(new KafkaHeaderImpl(USER_ID_HEADER, jobExecution.getUserId()));

    String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);

    return sendEventToKafka(params.getTenantId(), Json.encode(recordCollection), DI_RAW_RECORDS_CHUNK_PARSED.value(),
      kafkaHeaders, kafkaConfig, key)
      .map(parsedRecords);
  }

  private String prepareWrongJobProfileErrorMessage(JobExecution jobExecution, List<Record> records) {
    JobExecutionUtils.cache.put(jobExecution.getId(), JobExecution.Status.ERROR);
    return String.format(WRONG_JOB_PROFILE_ERROR_MESSAGE, jobExecution.getJobProfileInfo().getName(), records.get(0).getRecordType());
  }
}
