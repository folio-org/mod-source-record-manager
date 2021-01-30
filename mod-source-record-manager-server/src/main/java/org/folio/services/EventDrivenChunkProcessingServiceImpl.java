package org.folio.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dao.JobExecutionSourceChunkDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionProgress;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.mappers.processor.MappingParametersProvider;
import org.folio.services.progress.JobExecutionProgressService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.StatusDto.Status.PARSING_IN_PROGRESS;
import static org.folio.services.util.EventHandlingUtil.sendEventToKafka;

@Service("eventDrivenChunkProcessingService")
public class EventDrivenChunkProcessingServiceImpl extends AbstractChunkProcessingService {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventDrivenChunkProcessingServiceImpl.class);
  private static final AtomicInteger indexer = new AtomicInteger();

  private ChangeEngineService changeEngineService;
  private JobExecutionProgressService jobExecutionProgressService;
  private MappingParametersProvider mappingParametersProvider;
  private MappingRuleCache mappingRuleCache;
  private KafkaConfig kafkaConfig;

  @Value("${srm.kafka.CreatedRecordsKafkaHandler.maxDistributionNum:100}")
  private int maxDistributionNum;

  public EventDrivenChunkProcessingServiceImpl(@Autowired JobExecutionSourceChunkDao jobExecutionSourceChunkDao,
                                               @Autowired JobExecutionService jobExecutionService,
                                               @Autowired ChangeEngineService changeEngineService,
                                               @Autowired JobExecutionProgressService jobExecutionProgressService,
                                               @Autowired MappingParametersProvider mappingParametersProvider,
                                               @Autowired MappingRuleCache mappingRuleCache,
                                               @Autowired KafkaConfig kafkaConfig) {
    super(jobExecutionSourceChunkDao, jobExecutionService);
    this.changeEngineService = changeEngineService;
    this.jobExecutionProgressService = jobExecutionProgressService;
    this.mappingParametersProvider = mappingParametersProvider;
    this.mappingRuleCache = mappingRuleCache;
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
