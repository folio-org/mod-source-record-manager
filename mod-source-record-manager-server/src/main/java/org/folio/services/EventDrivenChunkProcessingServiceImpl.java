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
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionProgress;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.coordinator.QueuedBlockingCoordinator;
import org.folio.services.mappers.processor.MappingParametersProvider;
import org.folio.services.progress.JobExecutionProgressService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static java.lang.String.format;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.folio.rest.RestVerticle.MODULE_SPECIFIC_ARGS;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.StatusDto.Status.PARSING_IN_PROGRESS;
import static org.folio.services.util.EventHandlingUtil.sendEventWithPayload;

@Service("eventDrivenChunkProcessingService")
public class EventDrivenChunkProcessingServiceImpl extends AbstractChunkProcessingService {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventDrivenChunkProcessingServiceImpl.class);

  private static final int BLOCKING_COORDINATOR_RECORDS_NUMBER =
    Integer.parseInt(MODULE_SPECIFIC_ARGS.getOrDefault("record.publishing.blocking.coordinator.recors.number", "50"));

  private ChangeEngineService changeEngineService;
  private JobExecutionProgressService jobExecutionProgressService;
  private MappingParametersProvider mappingParametersProvider;
  private MappingRuleCache mappingRuleCache;
  private Vertx vertx;
  private QueuedBlockingCoordinator coordinator;

  public EventDrivenChunkProcessingServiceImpl(@Autowired JobExecutionSourceChunkDao jobExecutionSourceChunkDao,
                                               @Autowired JobExecutionService jobExecutionService,
                                               @Autowired ChangeEngineService changeEngineService,
                                               @Autowired JobExecutionProgressService jobExecutionProgressService,
                                               @Autowired MappingParametersProvider mappingParametersProvider,
                                               @Autowired MappingRuleCache mappingRuleCache,
                                               @Autowired Vertx vertx) {
    super(jobExecutionSourceChunkDao, jobExecutionService);
    this.changeEngineService = changeEngineService;
    this.jobExecutionProgressService = jobExecutionProgressService;
    this.mappingParametersProvider = mappingParametersProvider;
    this.mappingRuleCache = mappingRuleCache;
    this.vertx = vertx;
    this.coordinator = new QueuedBlockingCoordinator(BLOCKING_COORDINATOR_RECORDS_NUMBER);
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

  @Override
  public Future<Boolean> sendEventsWithStoredRecords(List<Record> createdRecords, String jobExecutionId, OkapiConnectionParams params) {
    return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
      .compose(jobOptional -> jobOptional
        .map(jobExecution -> getMappingParameters(jobExecutionId, params)
          .compose(mappingParameters -> mappingRuleCache.get(params.getTenantId())
            .compose(rulesOptional -> {
              if (rulesOptional.isPresent()) {
                return sendCreatedRecordsWithBlocking(createdRecords, jobExecution, rulesOptional.get(), mappingParameters, params);
              } else {
                return Future.failedFuture(format("Can not send events with created records, no mapping rules found for tenant %s", params.getTenantId()));
              }
            })))
        .orElse(Future.failedFuture(new NotFoundException(format("Couldn't find JobExecution with id %s", jobExecutionId)))));
  }

  private Future<Boolean> sendCreatedRecordsWithBlocking(List<Record> createdRecords, JobExecution jobExecution, JsonObject mappingRules, MappingParameters mappingParameters, OkapiConnectionParams params) {
    Promise<Boolean> promise = Promise.promise();
    List<Future> futures = new ArrayList<>();
    ProfileSnapshotWrapper profileSnapshotWrapper = new ObjectMapper().convertValue(jobExecution.getJobProfileSnapshotWrapper(), ProfileSnapshotWrapper.class);

    vertx.executeBlocking(blockingPromise -> {
      try {
        for (Record record : createdRecords) {
          coordinator.acceptLock();
          if (isRecordReadyToSend(record)) {
            DataImportEventPayload payload = prepareEventPayload(record, profileSnapshotWrapper, mappingRules, mappingParameters, params);
            Future<Boolean> booleanFuture = sendEventWithPayload(Json.encode(payload), DI_SRS_MARC_BIB_RECORD_CREATED.value(), params)
              .onComplete(ar -> coordinator.acceptUnlock());
            futures.add(booleanFuture);
          }
        }
      } catch (Exception e) {
        LOGGER.error("Error publishing event with record", e);
        futures.add(Future.failedFuture(e));
      }

      CompositeFuture.join(futures).onComplete(ar -> {
        if (ar.failed()) {
          LOGGER.error("Error publishing events with created records", ar.cause());
          promise.fail(ar.cause());
          return;
        }
        promise.complete(true);
      });
    }, null);

    return promise.future();
  }

  /**
   * Checks whether the record contains parsed content for sending.
   *
   * @param record record for verification
   * @return true if record has parsed content
   */
  private boolean isRecordReadyToSend(Record record) {
    if (record.getParsedRecord() == null || record.getParsedRecord().getContent() == null) {
      LOGGER.error("The record has not parsed content, so it will not be sent to mod-pubsub");
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
