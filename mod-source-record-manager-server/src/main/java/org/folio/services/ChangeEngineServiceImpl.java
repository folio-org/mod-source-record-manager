package org.folio.services;

import static java.lang.String.format;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import static org.folio.rest.RestVerticle.MODULE_SPECIFIC_ARGS;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_MARC_BIB_FOR_UPDATE_RECEIVED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_PARSED;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_HOLDING;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.TAG_999;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.addFieldToMarcRecord;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.getControlFieldValue;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.getValue;
import static org.folio.services.util.EventHandlingUtil.sendEventToKafka;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.ws.rs.NotFoundException;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.impl.KafkaHeaderImpl;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.marc.MarcRecordAnalyzer;
import org.folio.dataimport.util.marc.MarcRecordType;
import org.folio.dataimport.util.marc.RecordAnalyzer;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.rest.client.SourceStorageStreamClient;
import org.folio.rest.jaxrs.model.ActionProfile;
import org.folio.rest.jaxrs.model.ActionProfile.Action;
import org.folio.rest.jaxrs.model.ActionProfile.FolioRecord;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.JobProfileInfo.DataType;
import org.folio.rest.jaxrs.model.MarcRecordSearchRequest;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.RecordType;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsMetadata;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.afterprocessing.HrIdFieldService;
import org.folio.services.parsers.ParsedResult;
import org.folio.services.parsers.RecordParser;
import org.folio.services.parsers.RecordParserBuilder;

@Service
public class ChangeEngineServiceImpl implements ChangeEngineService {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final int THRESHOLD_CHUNK_SIZE =
    Integer.parseInt(MODULE_SPECIFIC_ARGS.getOrDefault("chunk.processing.threshold.chunk.size", "100"));
  private static final String TAG_001 = "001";
  private static final String TAG_004 = "004";
  private static final String MARC_FORMAT = "MARC_";
  private static final AtomicInteger indexer = new AtomicInteger();
  private static final String HOLDINGS_004_TAG_ERROR_MESSAGE =
    "The 004 tag of the Holdings doesn't has a link to the Bibliographic record";

  private JobExecutionSourceChunkDao jobExecutionSourceChunkDao;
  private JobExecutionService jobExecutionService;
  private RecordAnalyzer marcRecordAnalyzer;
  private HrIdFieldService hrIdFieldService;
  private RecordsPublishingService recordsPublishingService;
  private KafkaConfig kafkaConfig;

  @Value("${srm.kafka.RawChunksKafkaHandler.maxDistributionNum:100}")
  private int maxDistributionNum;

  public ChangeEngineServiceImpl(@Autowired JobExecutionSourceChunkDao jobExecutionSourceChunkDao,
                                 @Autowired JobExecutionService jobExecutionService,
                                 @Autowired MarcRecordAnalyzer marcRecordAnalyzer,
                                 @Autowired HrIdFieldService hrIdFieldService,
                                 @Autowired RecordsPublishingService recordsPublishingService,
                                 @Autowired KafkaConfig kafkaConfig) {
    this.jobExecutionSourceChunkDao = jobExecutionSourceChunkDao;
    this.jobExecutionService = jobExecutionService;
    this.marcRecordAnalyzer = marcRecordAnalyzer;
    this.hrIdFieldService = hrIdFieldService;
    this.recordsPublishingService = recordsPublishingService;
    this.kafkaConfig = kafkaConfig;
  }

  @Override
  public Future<List<Record>> parseRawRecordsChunkForJobExecution(RawRecordsDto chunk, JobExecution jobExecution,
                                                                  String sourceChunkId, OkapiConnectionParams params) {
    Promise<List<Record>> promise = Promise.promise();
    List<Record> parsedRecords =
      parseRecords(chunk.getInitialRecords(), chunk.getRecordsMetadata().getContentType(), jobExecution, sourceChunkId,
        params.getTenantId(), params);
    fillParsedRecordsWithAdditionalFields(parsedRecords);
    boolean updateMarcActionExists = containsUpdateMarcActionProfile(jobExecution.getJobProfileSnapshotWrapper());

    if (updateMarcActionExists) {
      LOGGER.info(
        "Records have not been saved in record-storage, because jobProfileSnapshotWrapper contains action for Marc-Bibliographic update");
      recordsPublishingService
        .sendEventsWithRecords(parsedRecords, jobExecution.getId(), params, DI_MARC_BIB_FOR_UPDATE_RECEIVED.value())
        .onSuccess(ar -> promise.complete(parsedRecords))
        .onFailure(promise::fail);
    } else {
      saveRecords(params, jobExecution, parsedRecords)
        .onComplete(postAr -> {
          if (postAr.failed()) {
            StatusDto statusDto = new StatusDto()
              .withStatus(StatusDto.Status.ERROR)
              .withErrorStatus(StatusDto.ErrorStatus.RECORD_UPDATE_ERROR);
            jobExecutionService.updateJobExecutionStatus(jobExecution.getId(), statusDto, params)
              .onComplete(r -> {
                if (r.failed()) {
                  LOGGER.error("Error during update jobExecution and snapshot status", r.cause());
                }
              });
            jobExecutionSourceChunkDao.getById(sourceChunkId, params.getTenantId())
              .compose(optional -> optional
                .map(sourceChunk -> jobExecutionSourceChunkDao
                  .update(sourceChunk.withState(JobExecutionSourceChunk.State.ERROR), params.getTenantId()))
                .orElseThrow(() -> new NotFoundException(String.format(
                  "Couldn't update failed jobExecutionSourceChunk status to ERROR, jobExecutionSourceChunk with id %s was not found",
                  sourceChunkId))))
              .onComplete(ar -> promise.fail(postAr.cause()));
          } else {
            promise.complete(parsedRecords);
          }
        });
    }
    return promise.future();
  }

  private boolean containsUpdateMarcActionProfile(ProfileSnapshotWrapper profileSnapshot) {
    List<ProfileSnapshotWrapper> childWrappers = profileSnapshot.getChildSnapshotWrappers();
    for (ProfileSnapshotWrapper childWrapper : childWrappers) {
      if (childWrapper.getContentType() == ProfileSnapshotWrapper.ContentType.ACTION_PROFILE
        && actionProfileMatches(childWrapper, FolioRecord.MARC_BIBLIOGRAPHIC, Action.UPDATE)) {
        return true;
      } else if (containsUpdateMarcActionProfile(childWrapper)) {
        return true;
      }
    }
    return false;
  }

  private boolean actionProfileMatches(ProfileSnapshotWrapper actionProfileWrapper, FolioRecord folioRecord, Action action) {
    ActionProfile actionProfile = new JsonObject((Map) actionProfileWrapper.getContent()).mapTo(ActionProfile.class);
    return actionProfile.getFolioRecord() == folioRecord && actionProfile.getAction() == action;
  }

  /**
   * Parse list of source records
   *
   * @param rawRecords    - list of raw records for parsing
   * @param jobExecution  - job execution of record's parsing
   * @param sourceChunkId - id of the JobExecutionSourceChunk
   * @param tenantId      - tenant id
   * @param okapiParams
   * @return - list of records with parsed or error data
   */
  private List<Record> parseRecords(List<InitialRecord> rawRecords, RecordsMetadata.ContentType recordContentType,
                                    JobExecution jobExecution, String sourceChunkId, String tenantId, OkapiConnectionParams okapiParams) {
    if (CollectionUtils.isEmpty(rawRecords)) {
      return Collections.emptyList();
    }
    RecordParser parser = RecordParserBuilder.buildParser(recordContentType);
    MutableInt counter = new MutableInt();
    // if number of records is more than THRESHOLD_CHUNK_SIZE update the progress every 20% of processed records,
    // otherwise update it once after all the records are processed
    int partition = rawRecords.size() > THRESHOLD_CHUNK_SIZE ? rawRecords.size() / 5 : rawRecords.size();
    return rawRecords.stream()
      .map(rawRecord -> {
        ParsedResult parsedResult = parser.parseRecord(rawRecord.getRecord());
        String recordId = UUID.randomUUID().toString();
        Record record = new Record()
          .withId(recordId)
          .withMatchedId(recordId)
          .withRecordType(inferRecordType(jobExecution, parsedResult, recordId))
          .withSnapshotId(jobExecution.getId())
          .withOrder(rawRecord.getOrder())
          .withGeneration(0)
          .withState(Record.State.ACTUAL)
          .withRawRecord(new RawRecord().withContent(rawRecord.getRecord()));
        if (parsedResult.isHasError()) {
          record.setErrorRecord(new ErrorRecord()
            .withContent(rawRecord)
            .withDescription(parsedResult.getErrors().encode()));
        } else {
          record.setParsedRecord(new ParsedRecord().withId(recordId).withContent(parsedResult.getParsedRecord().encode()));
          if (jobExecution.getJobProfileInfo().getDataType().equals(DataType.MARC)) {
            postProcessMarcRecord(record, rawRecord, okapiParams);
          }
        }
        return record;
      })
      .peek(stat -> { //NOSONAR
        if (counter.incrementAndGet() % partition == 0) {
          LOGGER.info("Parsed {} records out of {}", counter.intValue(), rawRecords.size());
          jobExecutionSourceChunkDao.getById(sourceChunkId, tenantId)
            .compose(optional -> optional
              .map(sourceChunk -> jobExecutionSourceChunkDao
                .update(sourceChunk.withProcessedAmount(sourceChunk.getProcessedAmount() + counter.intValue()), tenantId))
              .orElseThrow(() -> new NotFoundException(format(
                "Couldn't update jobExecutionSourceChunk progress, jobExecutionSourceChunk with id %s was not found",
                sourceChunkId))));
        }
      }).collect(Collectors.toList());
  }

  private void postProcessMarcRecord(Record record, InitialRecord rawRecord, OkapiConnectionParams okapiParams) {
    String matchedId = getValue(record, TAG_999, 's');
    if (StringUtils.isNotBlank(matchedId)) {
      record.setMatchedId(matchedId);
      record.setGeneration(null); // in case the same record is re-imported, generation should be calculated on SRS side
    }

    var recordType = record.getRecordType();
    if (recordType == MARC_BIB) {
      postProcessMarcBibRecord(record);
    } else if (recordType == MARC_HOLDING) {
      postProcessMarcHoldingsRecord(record, rawRecord, okapiParams);
    }
  }

  private void postProcessMarcBibRecord(Record record) {
    String instanceId = getValue(record, TAG_999, 'i');
    if (isNotBlank(instanceId)) {
      record.setExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId));
      String instanceHrid = getControlFieldValue(record, TAG_001);
      if (isNotBlank(instanceHrid)) {
        record.getExternalIdsHolder().setInstanceHrid(instanceHrid);
      }
    }
  }

  private void postProcessMarcHoldingsRecord(Record record, InitialRecord rawRecord, OkapiConnectionParams okapiParams) {
    var controlFieldValue = getControlFieldValue(record, TAG_004);
    if (isBlank(controlFieldValue)) {
      LOGGER.error(HOLDINGS_004_TAG_ERROR_MESSAGE);
      record.setParsedRecord(null);
      record.setErrorRecord(new ErrorRecord()
        .withContent(rawRecord)
        .withDescription(new JsonObject().put("message", HOLDINGS_004_TAG_ERROR_MESSAGE).encode())
      );
    } else {
      SourceStorageStreamClient sourceStorageStreamClient = getSourceStorageStreamClient(okapiParams);
      MarcRecordSearchRequest marcRecordSearchRequest = new MarcRecordSearchRequest();
      marcRecordSearchRequest.setFieldsSearchExpression("001.value = '" + controlFieldValue + "'");
      try {
        sourceStorageStreamClient.postSourceStorageStreamMarcRecordIdentifiers(marcRecordSearchRequest, asyncResult -> {
          if (asyncResult.succeeded()) {
            var body = asyncResult.result().body();
            LOGGER.info("Response from SRS with MARC Bib 001 field: {} and body: {}", controlFieldValue, body);
            var records = body.toJsonObject().getJsonArray("records");
            if (records.isEmpty()) {
              LOGGER.error(HOLDINGS_004_TAG_ERROR_MESSAGE);
              record.setParsedRecord(null);
              record.setErrorRecord(new ErrorRecord()
                .withContent(rawRecord)
                .withDescription(new JsonObject().put("message", HOLDINGS_004_TAG_ERROR_MESSAGE).encode()));
              throw new NotFoundException(HOLDINGS_004_TAG_ERROR_MESSAGE);
            }
          } else {
            LOGGER.error("Error during call post request to SRS");
          }
        });
      } catch (Exception e) {
        LOGGER.error("Error during call post request to SRS: {}", e.getMessage());
      }
    }
  }

  private SourceStorageStreamClient getSourceStorageStreamClient(OkapiConnectionParams okapiParams) {
    var token = okapiParams.getToken();
    var okapiUrl = okapiParams.getOkapiUrl();
    var tenantId = okapiParams.getTenantId();
    return new SourceStorageStreamClient(okapiUrl, tenantId, token);
  }

  private RecordType inferRecordType(JobExecution jobExecution, ParsedResult recordParsedResult, String recordId) {
    if (DataType.MARC.equals(jobExecution.getJobProfileInfo().getDataType())) {
      MarcRecordType marcRecordType = marcRecordAnalyzer.process(recordParsedResult.getParsedRecord());
      LOGGER.info("Marc record analyzer parsed record with id = {} and type = {}", recordId, marcRecordType);
      return RecordType.valueOf(MARC_FORMAT + marcRecordType.name());
    }

    return RecordType.valueOf(jobExecution.getJobProfileInfo().getDataType().value());
  }

  /**
   * Adds new additional fields into parsed records content to incoming records
   *
   * @param records list of records
   */
  private void fillParsedRecordsWithAdditionalFields(List<Record> records) {
    if (!CollectionUtils.isEmpty(records)) {
      Record.RecordType recordType = records.get(0).getRecordType();
      if (MARC_BIB.equals(recordType) || MARC_AUTHORITY.equals(recordType) || MARC_HOLDING.equals(recordType)) {
        hrIdFieldService.move001valueTo035Field(records);
        for (Record record : records) {
          addFieldToMarcRecord(record, TAG_999, 's', record.getMatchedId());
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

    RecordCollection recordCollection = new RecordCollection()
      .withRecords(parsedRecords)
      .withTotalRecords(parsedRecords.size());

    List<KafkaHeader> kafkaHeaders = KafkaHeaderUtils.kafkaHeadersFromMultiMap(params.getHeaders());

    kafkaHeaders.add(new KafkaHeaderImpl("jobExecutionId", jobExecution.getId()));

    String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);

    return sendEventToKafka(params.getTenantId(), Json.encode(recordCollection), DI_RAW_RECORDS_CHUNK_PARSED.value(),
      kafkaHeaders, kafkaConfig, key)
      .map(parsedRecords);
  }
}
