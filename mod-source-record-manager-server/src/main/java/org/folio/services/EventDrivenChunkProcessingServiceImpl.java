package org.folio.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.producer.KafkaHeader;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionProgress;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.mappers.processor.MappingParametersProvider;
import org.folio.services.progress.JobExecutionProgressService;
import org.folio.util.pubsub.PubSubClientUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.StatusDto.Status.PARSING_IN_PROGRESS;

@Service("eventDrivenChunkProcessingService")
public class EventDrivenChunkProcessingServiceImpl extends AbstractChunkProcessingService {
  private static final Logger LOGGER = LoggerFactory.getLogger(EventDrivenChunkProcessingServiceImpl.class);

  private ChangeEngineService changeEngineService;
  private JobExecutionProgressService jobExecutionProgressService;
  private MappingParametersProvider mappingParametersProvider;
  private MappingRuleService mappingRuleService;


  //TODO: make it configurable
  private int maxDistributionNum = 100;
  private static final AtomicInteger indexer = new AtomicInteger();
  private org.folio.kafka.KafkaConfig kafkaConfig;

  public EventDrivenChunkProcessingServiceImpl(@Autowired JobExecutionSourceChunkDao jobExecutionSourceChunkDao,
                                               @Autowired JobExecutionService jobExecutionService,
                                               @Autowired ChangeEngineService changeEngineService,
                                               @Autowired JobExecutionProgressService jobExecutionProgressService,
                                               @Autowired MappingParametersProvider mappingParametersProvider,
                                               @Autowired MappingRuleService mappingRuleService,
                                               @Autowired KafkaConfig kafkaConfig
  ) {
    super(jobExecutionSourceChunkDao, jobExecutionService);
    this.changeEngineService = changeEngineService;
    this.jobExecutionProgressService = jobExecutionProgressService;
    this.mappingParametersProvider = mappingParametersProvider;
    this.mappingRuleService = mappingRuleService;
    this.kafkaConfig = kafkaConfig;
  }

  @Override
  protected Future<Boolean> processRawRecordsChunk(RawRecordsDto incomingChunk, JobExecutionSourceChunk sourceChunk, String jobExecutionId, OkapiConnectionParams params) {
    Promise<Boolean> promise = Promise.promise();

    initializeJobExecutionProgressIfNecessary(jobExecutionId, incomingChunk, params.getTenantId())
      .compose(ar -> checkAndUpdateJobExecutionStatusIfNecessary(jobExecutionId, new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS), params))
      .compose(jobExec -> changeEngineService.parseRawRecordsChunkForJobExecution(incomingChunk, jobExec, sourceChunk.getId(), params))
      .onComplete(sendEventsAr -> updateJobExecutionIfAllSourceChunksMarkedAsError(jobExecutionId, params)
        .onComplete(updateAr -> promise.handle(sendEventsAr.map(true))));
    return promise.future();
  }

  private Future<Boolean> initializeJobExecutionProgressIfNecessary(String jobExecutionId, RawRecordsDto incomingChunk, String tenantId) {
    return jobExecutionService.getJobExecutionById(jobExecutionId, tenantId)
      .compose(optionalJobExecution -> optionalJobExecution
        .map(jobExecution -> {
          JobExecution.Status jobStatus = jobExecution.getStatus();
          if (PARSING_IN_PROGRESS.value().equals(jobStatus.value()) || StatusDto.Status.ERROR.value().equals(jobStatus.value())) {
            return Future.succeededFuture(true);
          }
          return jobExecutionProgressService.initializeJobExecutionProgress(jobExecution.getId(), incomingChunk.getRecordsMetadata().getTotal(), tenantId).map(true);
        }).orElse(Future.failedFuture(new NotFoundException(String.format("Couldn't find JobExecution with id %s", jobExecutionId)))));
  }

  /**
   * Sends events with created records to the mod-inventory
   *
   * @param createdRecords records to send
   * @param jobExecutionId job execution id
   * @param params         connection parameters
   * @return future with boolean
   */
  @Override
  public Future<Boolean> sendEventsWithStoredRecords(List<Record> createdRecords, String jobExecutionId, OkapiConnectionParams params) {
    String tenantId = params.getTenantId();
    Future<Optional<JobExecution>> jobExecutionFuture = jobExecutionService.getJobExecutionById(jobExecutionId, tenantId);
    Future<Optional<JsonObject>> mappingRulesFuture = mappingRuleService.get(params.getTenantId());
    Future<MappingParameters> mappingParameters = mappingParametersProvider.get(jobExecutionId, params);

    return CompositeFuture.all(jobExecutionFuture, mappingRulesFuture, mappingParameters).compose(cf -> {
      if (cf.failed()) {
        return Future.failedFuture(cf.cause());
      }

      Optional<JobExecution> jobExecutionOptional = jobExecutionFuture.result();
      if (Objects.isNull(jobExecutionOptional) || !jobExecutionOptional.isPresent()) {
        return Future.failedFuture(new NotFoundException(format("Couldn't find JobExecution with id %s", jobExecutionId)));
      }

      Optional<JsonObject> mappingRulesOptional = mappingRulesFuture.result();
      if (Objects.isNull(mappingRulesOptional) || !mappingRulesOptional.isPresent()) {
        return Future.failedFuture(format("Can not send events with created records, no mapping rules found for tenant %s", params.getTenantId()));
      }

      return sendCreatedRecordsWithBlocking(createdRecords, jobExecutionOptional.get(), mappingRulesOptional.get(), mappingParameters.result(), params);
    });
  }

  private Future<Boolean> sendEventWithPayload(String tenantId, String eventPayload, String eventType, List<KafkaHeader> kafkaHeaders, KafkaProducer<String, String> producer) {
    Event event;
    try {
      event = new Event()
        .withId(UUID.randomUUID().toString())
        .withEventType(eventType)
        .withEventPayload(ZIPArchiver.zip(eventPayload))
        .withEventMetadata(new EventMetadata()
          .withTenantId(tenantId)
          .withEventTTL(1)
          .withPublishedBy(PubSubClientUtils.constructModuleName()));
    } catch (IOException e) {
      e.printStackTrace();
      LOGGER.error(e);
      return Future.failedFuture(e);
    }

    String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);

    String topicName = KafkaTopicNameHelper.formatTopicName(kafkaConfig.getEnvId(), KafkaTopicNameHelper.getDefaultNameSpace(), tenantId, eventType);

    KafkaProducerRecord<String, String> record =
      KafkaProducerRecord.create(topicName, key, Json.encode(event));

    record.addHeaders(kafkaHeaders);

    Promise<Boolean> writePromise = Promise.promise();

    producer.write(record, war -> {
      if (war.succeeded()) {
        //TODO: this logic must be rewritten
        writePromise.complete(true);
      } else {
        Throwable cause = war.cause();
        LOGGER.error(producer + " write error:", cause);
        writePromise.fail(cause);
      }
    });

    return writePromise.future();
  }


  private Future<Boolean> sendCreatedRecordsWithBlocking(List<Record> createdRecords, JobExecution jobExecution, JsonObject mappingRules, MappingParameters mappingParameters, OkapiConnectionParams params) {
    Promise<Boolean> promise = Promise.promise();

    List<Future> futures = new ArrayList<>();
    ProfileSnapshotWrapper profileSnapshotWrapper = new ObjectMapper().convertValue(jobExecution.getJobProfileSnapshotWrapper(), ProfileSnapshotWrapper.class);

    List<KafkaHeader> kafkaHeaders = params
      .getHeaders()
      .entries()
      .stream()
      .map(e -> KafkaHeader.header(e.getKey(), e.getValue()))
      .collect(Collectors.toList());

    String eventType = DI_SRS_MARC_BIB_RECORD_CREATED.value();
    String producerName = eventType + "_Producer";
    KafkaProducer<String, String> producer =
      KafkaProducer.createShared(Vertx.currentContext().owner(), producerName, kafkaConfig.getProducerProps());

    for (Record record : createdRecords) {
      if (isRecordReadyToSend(record)) {
        DataImportEventPayload payload = prepareEventPayload(record, profileSnapshotWrapper, mappingRules, mappingParameters, params);
        Future<Boolean> booleanFuture = sendEventWithPayload(params.getTenantId(), Json.encode(payload), eventType, kafkaHeaders, producer);
        futures.add(booleanFuture);
      }
    }

    CompositeFuture.join(futures).onComplete(ar -> {
      producer.end(ear -> producer.close());
      if (ar.failed()) {
        LOGGER.error("Error publishing events with created records", ar.cause());
        promise.fail(ar.cause());
      } else {
        promise.complete(true);
      }
    });

    return promise.future();
  }

  /**
   * Checks whether the record contains parsed content for sending.
   *
   * @param record record for verification
   * @return true if record has parsed content
   */
  //TODO: What is the case for this F***ing method???
  private boolean isRecordReadyToSend(Record record) {
    if (record.getParsedRecord() == null || record.getParsedRecord().getContent() == null) {
      LOGGER.error("The record has not parsed content, so it will not be sent to mod-inventory!");
      return false;
    }
    return true;
  }

  /**
   * Provides external parameters for the MARC-to-Instance mapping process
   *
   * @param snapshotId  - snapshotId
   * @param okapiParams okapi connection parameters
   * @return mapping parameters
   */
  @Deprecated
  private Future<MappingParameters> getMappingParameters(String snapshotId, OkapiConnectionParams okapiParams) {
    return mappingParametersProvider.get(snapshotId, okapiParams);
  }

  /**
   * Prepares eventPayload with createdRecord and profileSnapshotWrapper
   *
   * @param createdRecord          record to send
   * @param profileSnapshotWrapper profileSnapshotWrapper to send
   * @param mappingRules           rules for default instance mapping
   * @param mappingParameters      mapping parameters
   * @param params                 connection parameters
   * @return dataImportEventPayload
   */
  private DataImportEventPayload prepareEventPayload(Record createdRecord, ProfileSnapshotWrapper profileSnapshotWrapper,
                                                     JsonObject mappingRules, MappingParameters mappingParameters, OkapiConnectionParams params) {
    HashMap<String, String> dataImportEventPayloadContext = new HashMap<>();
    dataImportEventPayloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(createdRecord));
    dataImportEventPayloadContext.put("MAPPING_RULES", mappingRules.encode());
    dataImportEventPayloadContext.put("MAPPING_PARAMS", Json.encode(mappingParameters));

    return new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_CREATED.value())
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withJobExecutionId(createdRecord.getSnapshotId())
      .withContext(dataImportEventPayloadContext)
      .withOkapiUrl(params.getOkapiUrl())
      .withTenant(params.getTenantId())
      .withToken(params.getToken());
  }

  private Future<Boolean> updateJobExecutionIfAllSourceChunksMarkedAsError(String jobExecutionId, OkapiConnectionParams params) {
    return jobExecutionSourceChunkDao.get("jobExecutionId=" + jobExecutionId + " AND last=true", 0, 1, params.getTenantId())
      .compose(chunks -> isNotEmpty(chunks) ? jobExecutionSourceChunkDao.isAllChunksProcessed(jobExecutionId, params.getTenantId()) : Future.succeededFuture(false))
      .compose(isAllChunksError -> {
        if (isAllChunksError) {
          StatusDto statusDto = new StatusDto().withStatus(StatusDto.Status.ERROR).withErrorStatus(StatusDto.ErrorStatus.RECORD_UPDATE_ERROR);
          return jobExecutionProgressService.getByJobExecutionId(jobExecutionId, params.getTenantId())
            .compose(progress -> updateJobExecutionState(jobExecutionId, progress, statusDto, params));
        }
        return Future.succeededFuture(false);
      });
  }

  private Future<Boolean> updateJobExecutionState(String jobExecutionId, JobExecutionProgress progress, StatusDto statusDto, OkapiConnectionParams params) {
    return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
      .compose(jobOptional -> jobOptional
        .map(jobExecution -> jobExecution
          .withStatus(JobExecution.Status.valueOf(statusDto.getStatus().value()))
          .withUiStatus(JobExecution.UiStatus.fromValue(Status.valueOf(statusDto.getStatus().value()).getUiStatus()))
          .withErrorStatus(JobExecution.ErrorStatus.valueOf(statusDto.getErrorStatus().value()))
          .withProgress(jobExecution.getProgress()
            .withCurrent(progress.getTotal())
            .withTotal(progress.getTotal())))
        .map(jobExecution -> jobExecutionService.updateJobExecutionWithSnapshotStatus(jobExecution, params).map(true))
        .orElse(Future.failedFuture(new NotFoundException(String.format("Couldn't find JobExecution to update with id %s", jobExecutionId)))));
  }
}
