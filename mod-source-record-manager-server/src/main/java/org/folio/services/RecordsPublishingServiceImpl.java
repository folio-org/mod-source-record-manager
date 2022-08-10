package org.folio.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.exceptions.RawChunkRecordsParsingException;
import org.folio.services.exceptions.RecordsPublishingException;
import org.folio.services.util.EventHandlingUtil;
import org.folio.verticle.consumers.errorhandlers.payloadbuilders.DiErrorPayloadBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.services.util.EventHandlingUtil.sendEventToKafka;

@Service("recordsPublishingService")
  public class RecordsPublishingServiceImpl implements RecordsPublishingService {

  private static final Logger LOGGER = LogManager.getLogger();
  public static final String RECORD_ID_HEADER = "recordId";
  public static final String USER_ID_HEADER = "userId";
  private static final AtomicInteger indexer = new AtomicInteger();

  public static final String ERROR_KEY = "ERROR";

  private JobExecutionService jobExecutionService;
  private DataImportPayloadContextBuilder payloadContextBuilder;
  private KafkaConfig kafkaConfig;

  @Value("${srm.kafka.CreatedRecordsKafkaHandler.maxDistributionNum:100}")
  private int maxDistributionNum;
  @Autowired
  private List<DiErrorPayloadBuilder> errorPayloadBuilders;

  public RecordsPublishingServiceImpl(@Autowired JobExecutionService jobExecutionService,
                                      @Autowired DataImportPayloadContextBuilder payloadContextBuilder,
                                      @Autowired KafkaConfig kafkaConfig) {
    this.jobExecutionService = jobExecutionService;
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
    List<Record> failedRecords = new ArrayList<>();
    ProfileSnapshotWrapper profileSnapshotWrapper = new ObjectMapper().convertValue(jobExecution.getJobProfileSnapshotWrapper(), ProfileSnapshotWrapper.class);

    for (Record record : createdRecords) {
      String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);
      try {
        if (isParsedContentExists(record)) {
          DataImportEventPayload payload = prepareEventPayload(record, profileSnapshotWrapper, params, eventType);
          params.getHeaders().set(RECORD_ID_HEADER, record.getId());
          params.getHeaders().set(USER_ID_HEADER, jobExecution.getUserId());
          futures.add(sendEventToKafka(params.getTenantId(), Json.encode(payload),
            eventType, KafkaHeaderUtils.kafkaHeadersFromMultiMap(params.getHeaders()), kafkaConfig, key));
        }
        else {
          sendDiErrorEvent(new RawChunkRecordsParsingException(record.getErrorRecord().getDescription()),
            params, jobExecution.getId(), params.getTenantId(), record);
        }
      } catch (Exception e) {
        LOGGER.error("Error publishing event with record id: {}",record.getId(), e);
        record.setErrorRecord(new ErrorRecord().withContent(record.getRawRecord()).withDescription(e.getMessage()));
        failedRecords.add(record);
      }
    }

    if (CollectionUtils.isNotEmpty(failedRecords)) {
      futures.add(Future.failedFuture(new RecordsPublishingException(String.format("Failed to process %s records", failedRecords.size()), failedRecords)));
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
   * @param currentRecord record for verification
   * @return true if record has parsed content
   */
  private boolean isParsedContentExists(Record currentRecord) {
    if (currentRecord.getParsedRecord() == null || currentRecord.getParsedRecord().getContent() == null) {
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
    HashMap<String, String> context = payloadContextBuilder.buildFrom(record, profileSnapshotWrapper.getId());

    return new DataImportEventPayload()
      .withEventType(eventType)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withJobExecutionId(record.getSnapshotId())
      .withContext(context)
      .withOkapiUrl(params.getOkapiUrl())
      .withTenant(params.getTenantId())
      .withToken(params.getToken());
  }

  public void sendDiErrorEvent(Throwable throwable,
                               OkapiConnectionParams okapiParams,
                               String jobExecutionId,
                               String tenantId,
                               Record currentRecord) {
      okapiParams.getHeaders().set(RECORD_ID_HEADER, currentRecord.getId());
      for (DiErrorPayloadBuilder payloadBuilder: errorPayloadBuilders) {
        if (payloadBuilder.isEligible(currentRecord.getRecordType())) {
          LOGGER.info("Start building DI_ERROR payload for jobExecutionId {} and recordId {}", jobExecutionId, currentRecord.getId());
          payloadBuilder.buildEventPayload(throwable, okapiParams, jobExecutionId, currentRecord)
            .compose(payload -> EventHandlingUtil.sendEventToKafka(tenantId, Json.encode(payload), DI_ERROR.value(),
              KafkaHeaderUtils.kafkaHeadersFromMultiMap(okapiParams.getHeaders()), kafkaConfig, null));
          return;
        }
      }
      LOGGER.warn("Appropriate DI_ERROR payload builder not found, DI_ERROR without records info will be send");
  }
}
