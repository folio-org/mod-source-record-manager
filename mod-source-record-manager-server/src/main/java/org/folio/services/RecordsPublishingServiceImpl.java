package org.folio.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.folio.okapi.common.GenericCompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.mappers.processor.MappingParametersProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static org.folio.rest.jaxrs.model.EntityType.EDIFACT_INVOICE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.services.util.EventHandlingUtil.sendEventToKafka;

@Service("recordsPublishingService")
public class RecordsPublishingServiceImpl implements RecordsPublishingService {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final AtomicInteger indexer = new AtomicInteger();

  private JobExecutionService jobExecutionService;
  private MappingParametersProvider mappingParametersProvider;
  private MappingRuleCache mappingRuleCache;
  private KafkaConfig kafkaConfig;

  @Value("${srm.kafka.CreatedRecordsKafkaHandler.maxDistributionNum:100}")
  private int maxDistributionNum;

  public RecordsPublishingServiceImpl(@Autowired JobExecutionService jobExecutionService,
                                      @Autowired MappingParametersProvider mappingParametersProvider,
                                      @Autowired MappingRuleCache mappingRuleCache,
                                      @Autowired KafkaConfig kafkaConfig) {
    this.jobExecutionService = jobExecutionService;
    this.mappingParametersProvider = mappingParametersProvider;
    this.mappingRuleCache = mappingRuleCache;
    this.kafkaConfig = kafkaConfig;
  }

  @Override
  public Future<Boolean> sendEventsWithRecords(List<Record> records, String jobExecutionId, OkapiConnectionParams params, String eventType) {
    return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
      .compose(jobOptional -> jobOptional
        .map(jobExecution -> getMappingParameters(jobExecutionId, params)
          .compose(mappingParameters -> mappingRuleCache.get(params.getTenantId())
            .compose(rulesOptional -> {
              if (rulesOptional.isPresent()) {
                return sendRecords(records, jobExecution, rulesOptional.get(), mappingParameters, params, eventType);
              } else {
                return Future.failedFuture(format("Can not send events with records, no mapping rules found for tenant %s", params.getTenantId()));
              }
            })))
        .orElse(Future.failedFuture(new NotFoundException(format("Couldn't find JobExecution with id %s", jobExecutionId)))));
  }

  private Future<Boolean> sendRecords(List<Record> createdRecords, JobExecution jobExecution, JsonObject mappingRules, MappingParameters mappingParameters, OkapiConnectionParams params, String eventType) {
    Promise<Boolean> promise = Promise.promise();
    List<Future<Boolean>> futures = new ArrayList<>();
    ProfileSnapshotWrapper profileSnapshotWrapper = new ObjectMapper().convertValue(jobExecution.getJobProfileSnapshotWrapper(), ProfileSnapshotWrapper.class);
    try {
      for (Record record : createdRecords) {
        if (isRecordReadyToSend(record)) {
          String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);
          DataImportEventPayload payload = prepareEventPayload(record, profileSnapshotWrapper, mappingRules, mappingParameters, params, eventType);
          Future<Boolean> booleanFuture = sendEventToKafka(params.getTenantId(), Json.encode(payload),
            eventType, KafkaHeaderUtils.kafkaHeadersFromMultiMap(params.getHeaders()), kafkaConfig, key);
          futures.add(booleanFuture);
        }
      }
    } catch (Exception e) {
      LOGGER.error("Error publishing event with record", e);
      futures.add(Future.failedFuture(e));
    }

    GenericCompositeFuture.join(futures).onComplete(ar -> {
      if (ar.failed()) {
        LOGGER.error("Error publishing events with records", ar.cause());
        promise.fail(ar.cause());
        return;
      }
      promise.complete(true);
    });
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
      LOGGER.error("Record has no parsed content - event will not be sent");
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
   * Prepares eventPayload with record and profileSnapshotWrapper
   *
   * @param record                 record to send
   * @param profileSnapshotWrapper profileSnapshotWrapper to send
   * @param mappingRules           rules for default instance mapping
   * @param mappingParameters      mapping parameters
   * @param params                 connection parameters
   * @return dataImportEventPayload
   */
  private DataImportEventPayload prepareEventPayload(Record record, ProfileSnapshotWrapper profileSnapshotWrapper,
                                                     JsonObject mappingRules, MappingParameters mappingParameters, OkapiConnectionParams params,
                                                     String eventType) {
    HashMap<String, String> dataImportEventPayloadContext = new HashMap<>();
    dataImportEventPayloadContext.put(inferEntityType(record).value(), Json.encode(record));
    dataImportEventPayloadContext.put("MAPPING_RULES", mappingRules.encode());
    dataImportEventPayloadContext.put("MAPPING_PARAMS", Json.encode(mappingParameters));

    return new DataImportEventPayload()
      .withEventType(eventType)
      .withProfileSnapshot(profileSnapshotWrapper)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withJobExecutionId(record.getSnapshotId())
      .withContext(dataImportEventPayloadContext)
      .withOkapiUrl(params.getOkapiUrl())
      .withTenant(params.getTenantId())
      .withToken(params.getToken());
  }

  private EntityType inferEntityType(Record record) {
    switch (record.getRecordType()) {
      case EDIFACT:
        return EDIFACT_INVOICE;
      case MARC:
      default:
        return MARC_BIBLIOGRAPHIC;
    }
  }
}
