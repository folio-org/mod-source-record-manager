package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.mutable.MutableInt;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.ActionProfile;
import org.folio.rest.jaxrs.model.ActionProfile.Action;
import org.folio.rest.jaxrs.model.ActionProfile.FolioRecord;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordCollection;
import org.folio.rest.jaxrs.model.RecordsMetadata;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.afterprocessing.HrIdFieldService;
import org.folio.services.journal.JournalService;
import org.folio.services.parsers.ParsedResult;
import org.folio.services.parsers.RecordParser;
import org.folio.services.parsers.RecordParserBuilder;
import org.folio.util.pubsub.PubSubClientUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.folio.rest.RestVerticle.MODULE_SPECIFIC_ARGS;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.CREATE;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.TAG_999;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.addFieldToMarcRecord;
import static org.folio.services.afterprocessing.AdditionalFieldsUtil.getValue;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_MARC_BIB_RECORDS_CHUNK_PARSED;

@Service
public class ChangeEngineServiceImpl implements ChangeEngineService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ChangeEngineServiceImpl.class);
  private static final int THRESHOLD_CHUNK_SIZE =
    Integer.parseInt(MODULE_SPECIFIC_ARGS.getOrDefault("chunk.processing.threshold.chunk.size", "100"));

  private JobExecutionSourceChunkDao jobExecutionSourceChunkDao;
  private JobExecutionService jobExecutionService;
  private JournalService journalService;
  private HrIdFieldService hrIdFieldService;
  private KafkaConfig kafkaConfig;

  //TODO: make it configurable
  private int maxDistributionNum = 100;
  private static final AtomicInteger indexer = new AtomicInteger();

  public ChangeEngineServiceImpl(@Autowired JobExecutionSourceChunkDao jobExecutionSourceChunkDao,
                                 @Autowired JobExecutionService jobExecutionService,
                                 @Autowired HrIdFieldService hrIdFieldService,
                                 @Autowired @Qualifier("journalServiceProxy") JournalService journalService,
                                 @Autowired KafkaConfig kafkaConfig) {
    this.jobExecutionSourceChunkDao = jobExecutionSourceChunkDao;
    this.jobExecutionService = jobExecutionService;
    this.journalService = journalService;
    this.hrIdFieldService = hrIdFieldService;
    this.kafkaConfig = kafkaConfig;
  }

  @Override
  public Future<List<Record>> parseRawRecordsChunkForJobExecution(RawRecordsDto chunk, JobExecution jobExecution, String sourceChunkId, OkapiConnectionParams params) {
    Promise<List<Record>> promise = Promise.promise();
    List<Record> parsedRecords = parseRecords(chunk.getInitialRecords(), chunk.getRecordsMetadata().getContentType(), jobExecution, sourceChunkId, params.getTenantId());
    fillParsedRecordsWithAdditionalFields(parsedRecords);
    boolean updateMarcActionExists = containsUpdateMarcActionProfile(jobExecution.getJobProfileSnapshotWrapper());

    if (updateMarcActionExists) {
      LOGGER.info("Records have not been sent to the record-storage, because jobProfileSnapshotWrapper contains action for Marc-Bibliographic update");
      promise.complete(parsedRecords);
    } else {
      postRecords(params, jobExecution, parsedRecords)
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
                .map(sourceChunk -> jobExecutionSourceChunkDao.update(sourceChunk.withState(JobExecutionSourceChunk.State.ERROR), params.getTenantId()))
                .orElseThrow(() -> new NotFoundException(String.format(
                  "Couldn't update failed jobExecutionSourceChunk status to ERROR, jobExecutionSourceChunk with id %s was not found", sourceChunkId))))
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
   * @return - list of records with parsed or error data
   */
  private List<Record> parseRecords(List<InitialRecord> rawRecords, RecordsMetadata.ContentType recordContentType, JobExecution jobExecution, String sourceChunkId, String tenantId) {
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
          .withRecordType(Record.RecordType.valueOf(jobExecution.getJobProfileInfo().getDataType().value()))
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
          String matchedId = getValue(record, "999", 's');
          if (matchedId != null){
            record.setMatchedId(matchedId);
            record.setGeneration(null); // in case the same record is re-imported, generation should be calculated on SRS side
          }
        }
        return record;
      })
      .peek(stat -> { //NOSONAR
        if (counter.incrementAndGet() % partition == 0) {
          LOGGER.info("Parsed {} records out of {}", counter.intValue(), rawRecords.size());
          jobExecutionSourceChunkDao.getById(sourceChunkId, tenantId)
            .compose(optional -> optional
              .map(sourceChunk -> jobExecutionSourceChunkDao.update(sourceChunk.withProcessedAmount(sourceChunk.getProcessedAmount() + counter.intValue()), tenantId))
              .orElseThrow(() -> new NotFoundException(format(
                "Couldn't update jobExecutionSourceChunk progress, jobExecutionSourceChunk with id %s was not found", sourceChunkId))));
        }
      }).collect(Collectors.toList());
  }

  /**
   * Adds new additional fields into parsed records content to incoming records
   *
   * @param records list of records
   */
  private void fillParsedRecordsWithAdditionalFields(List<Record> records) {
    if (!CollectionUtils.isEmpty(records)) {
      Record.RecordType recordType = records.get(0).getRecordType();
      if (Record.RecordType.MARC.equals(recordType)) {
        hrIdFieldService.move001valueTo035Field(records);
        for (Record record : records) {
          addFieldToMarcRecord(record, TAG_999, 's', record.getMatchedId());
        }
      }
    }
  }

  /**
   * Saves parsed records in record-storage
   *
   * @param params        - okapi params for connecting record-storage
   * @param jobExecution  - job execution related to records
   * @param parsedRecords - parsed records
   */
  private Future<List<Record>> postRecords(OkapiConnectionParams params, JobExecution jobExecution, List<Record> parsedRecords) {
    if (CollectionUtils.isEmpty(parsedRecords)) {
      return Future.succeededFuture();
    }

    RecordCollection recordCollection = new RecordCollection()
      .withRecords(parsedRecords)
      .withTotalRecords(parsedRecords.size());

    //TODO: this could be some utility method
    Event event;
    try {
      event = new Event()
        .withId(UUID.randomUUID().toString())
        .withEventType(DI_RAW_MARC_BIB_RECORDS_CHUNK_PARSED.value())
        .withEventPayload(ZIPArchiver.zip(Json.encode(recordCollection)))
        .withEventMetadata(new EventMetadata()
          .withTenantId(params.getTenantId())
          .withEventTTL(1)
          .withPublishedBy(PubSubClientUtils.constructModuleName()));
    } catch (IOException e) {
      e.printStackTrace();
      LOGGER.error(e);
      return Future.failedFuture(e);
    }

    String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);

    String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), KafkaTopicNameHelper.getDefaultNameSpace(),
      params.getTenantId(), DI_RAW_MARC_BIB_RECORDS_CHUNK_PARSED.value());

    KafkaProducerRecord<String, String> record =
      KafkaProducerRecord.create(topicName, key, Json.encode(event));

    List<KafkaHeader> kafkaHeaders = params
      .getHeaders()
      .entries()
      .stream()
      .map(e -> KafkaHeader.header(e.getKey(), e.getValue()))
      .collect(Collectors.toList());

    record.addHeaders(kafkaHeaders);
    record.addHeader("jobExecutionId", jobExecution.getId());

    Promise<List<Record>> writePromise = Promise.promise();

    String producerName = DI_RAW_MARC_BIB_RECORDS_CHUNK_PARSED + "_Producer";
    KafkaProducer<String, String> producer =
      KafkaProducer.createShared(Vertx.currentContext().owner(), producerName, kafkaConfig.getProducerProps());

    producer.write(record, war -> {
      producer.end(ear -> producer.close());
      if (war.succeeded()) {
        //TODO: this logic must be rewritten
        List<JsonObject> journalRecords = buildJournalRecordsForProcessedRecords(parsedRecords, parsedRecords, CREATE);
        journalService.saveBatch(new JsonArray(journalRecords), params.getTenantId());

        writePromise.complete();
      } else {
        Throwable cause = war.cause();
        LOGGER.error("{} write error:", cause, producerName);
        writePromise.fail(cause);
      }
    });

    return writePromise.future();
  }

  /**
   * Builds list of journal records represented as json objects,
   * which contain info about records processing result
   *
   * @param records          records that should be created
   * @param processedRecords created records
   * @param actionType       action type which was performed on instances during processing
   * @return list of journal records represented as json objects
   */
  private List<JsonObject> buildJournalRecordsForProcessedRecords(List<Record> records, List<Record> processedRecords,
                                                                  JournalRecord.ActionType actionType) {
    Set<String> createdRecordIds = processedRecords.stream()
      .map(Record::getId)
      .collect(Collectors.toSet());

    List<JsonObject> journalRecords = new ArrayList<>();
    records.forEach(record -> {
      JournalRecord journalRecord = new JournalRecord()
        .withJobExecutionId(record.getSnapshotId())
        .withSourceRecordOrder(record.getOrder())
        .withSourceId(record.getId())
        .withEntityType(JournalRecord.EntityType.MARC_BIBLIOGRAPHIC)
        .withEntityId(record.getId())
        .withActionType(actionType)
        .withActionDate(new Date())
        .withActionStatus(record.getErrorRecord() == null && createdRecordIds.contains(record.getId())
          ? JournalRecord.ActionStatus.COMPLETED
          : JournalRecord.ActionStatus.ERROR);

      journalRecords.add(JsonObject.mapFrom(journalRecord));
    });
    return journalRecords;
  }
}
