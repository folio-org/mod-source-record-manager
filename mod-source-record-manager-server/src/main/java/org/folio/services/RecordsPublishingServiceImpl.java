package org.folio.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.JobExecution;
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
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.EntityType.EDIFACT_INVOICE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.EntityType.MARC_HOLDINGS;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_HOLDING;
import static org.folio.services.util.EventHandlingUtil.sendEventToKafka;

@Service("recordsPublishingService")
  public class RecordsPublishingServiceImpl implements RecordsPublishingService {

  private static final Logger LOGGER = LogManager.getLogger();
  public static final String CORRELATION_ID_HEADER = "correlationId";
  private static final String ERROR_MSG_KEY = "ERROR";
  private static final AtomicInteger indexer = new AtomicInteger();

  private JobExecutionService jobExecutionService;
  private MappingParametersProvider mappingParametersProvider;
  private MappingRuleCache mappingRuleCache;
  private DataImportPayloadContextBuilder payloadContextBuilder;
  private KafkaConfig kafkaConfig;

  @Value("${srm.kafka.CreatedRecordsKafkaHandler.maxDistributionNum:100}")
  private int maxDistributionNum;

  public RecordsPublishingServiceImpl(@Autowired JobExecutionService jobExecutionService,
                                      @Autowired MappingParametersProvider mappingParametersProvider,
                                      @Autowired MappingRuleCache mappingRuleCache,
                                      @Autowired DataImportPayloadContextBuilder payloadContextBuilder,
                                      @Autowired KafkaConfig kafkaConfig) {
    this.jobExecutionService = jobExecutionService;
    this.mappingParametersProvider = mappingParametersProvider;
    this.mappingRuleCache = mappingRuleCache;
    this.payloadContextBuilder = payloadContextBuilder;
    this.kafkaConfig = kafkaConfig;
  }

  @Override
  public Future<Boolean> sendEventsWithRecords(List<Record> records, String jobExecutionId, OkapiConnectionParams params, String eventType) {
    return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
      .compose(jobExecutionOptional -> {
        if (jobExecutionOptional.isPresent()) {
          return sendRecords(records, jobExecutionOptional.get(), params, eventType);
        } else {
          return Future.failedFuture(new NotFoundException(format("Couldn't find JobExecution with id %s", jobExecutionId)));
        }
      });
  }

  private Future<Boolean> sendRecords(List<Record> createdRecords, JobExecution jobExecution, OkapiConnectionParams params, String eventType) {
    Promise<Boolean> promise = Promise.promise();
    List<Future<Boolean>> futures = new ArrayList<>();
    ProfileSnapshotWrapper profileSnapshotWrapper = new ObjectMapper().convertValue(jobExecution.getJobProfileSnapshotWrapper(), ProfileSnapshotWrapper.class);

    for (Record record : createdRecords) {
      String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);
      try {
        if (isRecordReadyToSend(record)) {
          DataImportEventPayload payload = prepareEventPayload(record, profileSnapshotWrapper, params, eventType);
          params.getHeaders().set(CORRELATION_ID_HEADER, UUID.randomUUID().toString());
          Future<Boolean> booleanFuture = sendEventToKafka(params.getTenantId(), Json.encode(payload),
            eventType, KafkaHeaderUtils.kafkaHeadersFromMultiMap(params.getHeaders()), kafkaConfig, key);
          futures.add(booleanFuture.onFailure(th -> sendEventWithRecordPublishingError(record, jobExecution, params, th.getMessage(), kafkaConfig, key)));
        }
      } catch (Exception e) {
        LOGGER.error("Error publishing event with record", e);
        futures.add(Future.<Boolean>failedFuture(e)
          .onFailure(th -> sendEventWithRecordPublishingError(record, jobExecution, params, th.getMessage(), kafkaConfig, key)));
      }
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

  private void sendEventWithRecordPublishingError(Record record, JobExecution jobExecution, OkapiConnectionParams params, String errorMsg, KafkaConfig kafkaConfig, String key) {
    String sourceRecordKey = getSourceRecordKey(record);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withProfileSnapshot(jobExecution.getJobProfileSnapshotWrapper())
      .withJobExecutionId(record.getSnapshotId())
      .withOkapiUrl(params.getOkapiUrl())
      .withTenant(params.getTenantId())
      .withToken(params.getToken())
      .withContext(new HashMap<>() {{
        put(sourceRecordKey, Json.encode(record));
        put(ERROR_MSG_KEY, errorMsg);
      }});

    sendEventToKafka(params.getTenantId(), Json.encode(eventPayload), DI_ERROR.value(), KafkaHeaderUtils.kafkaHeadersFromMultiMap(params.getHeaders()), kafkaConfig, key)
      .onFailure(th -> LOGGER.error("Error publishing DI_ERROR event for record with id {}", record.getId(), th));
  }

  private String getSourceRecordKey(Record record) {
    switch (record.getRecordType()) {
      case MARC_BIB:
        return MARC_BIBLIOGRAPHIC.value();
      case MARC_AUTHORITY:
        return MARC_AUTHORITY.value();
      case MARC_HOLDING:
        return MARC_HOLDINGS.value();
      case EDIFACT:
      default:
        return EDIFACT_INVOICE.value();
    }
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
   * Prepares eventPayload with record and profileSnapshotWrapper
   *
   * @param record                 record to send
   * @param profileSnapshotWrapper profileSnapshotWrapper to send
   * @param params                 connection parameters
   * @return dataImportEventPayload
   */
  private DataImportEventPayload prepareEventPayload(Record record, ProfileSnapshotWrapper profileSnapshotWrapper,
                                                     OkapiConnectionParams params, String eventType) {
    HashMap<String, String> context = payloadContextBuilder.buildFrom(record);

    return new DataImportEventPayload()
      .withEventType(eventType)
      .withCurrentNode(
        // TODO check for Holdings should be removed after implementing linkage with mod-inventory holdings records
        MARC_AUTHORITY.equals(record.getRecordType()) || MARC_HOLDING.equals(record.getRecordType())
          ? new ProfileSnapshotWrapper()
          : profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withJobExecutionId(record.getSnapshotId())
      .withContext(context)
      .withOkapiUrl(params.getOkapiUrl())
      .withTenant(params.getTenantId())
      .withToken(params.getToken());
  }

}
