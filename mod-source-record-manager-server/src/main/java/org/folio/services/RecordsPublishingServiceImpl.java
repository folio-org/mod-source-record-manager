package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.collections4.CollectionUtils;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.exceptions.RawChunkRecordsParsingException;
import org.folio.services.exceptions.RecordsPublishingException;
import org.folio.services.util.EventHandlingUtil;
import org.folio.services.util.RecordConversionUtil;
import org.folio.verticle.consumers.errorhandlers.payloadbuilders.DiErrorPayloadBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.services.journal.JournalUtil.INCOMING_RECORD_ID;
import static org.folio.services.util.EventHandlingUtil.sendEventToKafka;

@Log4j2
@Service("recordsPublishingService")
public class RecordsPublishingServiceImpl implements RecordsPublishingService {

  public static final String RECORD_ID_HEADER = "recordId";
  public static final String USER_ID_HEADER = "userId";
  private static final String JOB_EXECUTION_ID_HEADER = "jobExecutionId";
  private static final AtomicInteger indexer = new AtomicInteger();
  private static final String ERROR_KEY = "ERROR";

  @Value("${srm.kafka.CreatedRecordsKafkaHandler.maxDistributionNum:100}")
  private int maxDistributionNum;

  private final JobExecutionService jobExecutionService;
  private final DataImportPayloadContextBuilder payloadContextBuilder;
  private final KafkaConfig kafkaConfig;
  private final List<DiErrorPayloadBuilder> errorPayloadBuilders;

  public RecordsPublishingServiceImpl(JobExecutionService jobExecutionService,
                                      DataImportPayloadContextBuilder payloadContextBuilder,
                                      KafkaConfig kafkaConfig,
                                      List<DiErrorPayloadBuilder> errorPayloadBuilders) {
    this.jobExecutionService = jobExecutionService;
    this.payloadContextBuilder = payloadContextBuilder;
    this.kafkaConfig = kafkaConfig;
    this.errorPayloadBuilders = errorPayloadBuilders;
  }

  @Override
  public Future<Boolean> sendEventsWithRecords(List<Record> records, String jobExecutionId, ConnectionParams params, String eventType, Map<String, String> context) {
    return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
      .compose(jobExecutionOptional -> {
        if (jobExecutionOptional.isPresent()) {
          return sendRecords(records, jobExecutionOptional.get(), params, eventType, context);
        } else {
          return Future.failedFuture(new NotFoundException(format("Couldn't find JobExecution with id %s", jobExecutionId)));
        }
      });
  }

  private Future<Boolean> sendRecords(List<Record> createdRecords, JobExecution jobExecution, ConnectionParams params, String eventType, Map<String, String> context) {
    log.debug("sendRecords:: Sending events with records for jobExecutionId: {} and records count: {}", jobExecution.getId(), createdRecords.size());
    Promise<Boolean> promise = Promise.promise();
    List<Future<Boolean>> futures = new ArrayList<>();
    List<Record> failedRecords = new ArrayList<>();
    ProfileSnapshotWrapper profileSnapshotWrapper = DatabindCodec.mapper().convertValue(jobExecution.getJobProfileSnapshotWrapper(), ProfileSnapshotWrapper.class);

    for (Record record : createdRecords) {
      String key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);
      try {
        if (record.getRecordType() != null && isParsedContentExists(record)) {
          log.debug("sendRecords:: Prepared event payload for recordId: {} and jobExecutionId: {}", record.getId(), jobExecution.getId());
          DataImportEventPayload payload = prepareEventPayload(record, profileSnapshotWrapper, params, eventType, context);
          params.getHeaders().put(RECORD_ID_HEADER, record.getId());
          params.getHeaders().put(JOB_EXECUTION_ID_HEADER, record.getSnapshotId());
          params.getHeaders().put(USER_ID_HEADER, jobExecution.getUserId());
          futures.add(sendEventToKafka(params.getTenantId(), Json.encode(payload),
            eventType, KafkaHeaderUtils.kafkaHeadersFromMap(params.getHeaders()), kafkaConfig, key));
        } else {
          String cause = record.getErrorRecord() == null
            ? format("Cannot send event for individual record with recordType: %s", record.getRecordType())
            : record.getErrorRecord().getDescription();
          log.error("sendRecords:: Error preparing event payload for recordId: {} and jobExecutionId: {}. Cause: {}", record.getId(), jobExecution.getId(), cause);
          futures.add(sendDiErrorEvent(new RawChunkRecordsParsingException(cause),
            params, jobExecution.getId(), params.getTenantId(), record));
        }
      } catch (Exception e) {
        log.error("sendRecords:: Error publishing event with jobExecutionId: {} recordId: {}", jobExecution.getId(), record.getId(), e);
        record.setErrorRecord(new ErrorRecord().withContent(record.getRawRecord()).withDescription(e.getMessage()));
        failedRecords.add(record);
      }
    }

    if (CollectionUtils.isNotEmpty(failedRecords)) {
      futures.add(Future.failedFuture(new RecordsPublishingException(String.format("Failed to process %s records", failedRecords.size()), failedRecords)));
    }

    Future.join(futures).onComplete(ar -> {
      if (ar.failed()) {
        log.warn("sendRecords:: Error publishing events with records for jobExecutionId: {}", jobExecution.getId(), ar.cause());
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
      log.warn("isParsedContentExists:: Record has no parsed content - event will not be sent for recordId: {}", currentRecord.getId());
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
                                                     ConnectionParams params, String eventType, Map<String, String> contextParams) {
    HashMap<String, String> context = payloadContextBuilder.buildFrom(record, profileSnapshotWrapper.getId());
    Optional.ofNullable(contextParams)
      .ifPresent(context::putAll);

    return new DataImportEventPayload()
      .withEventType(eventType)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withJobExecutionId(record.getSnapshotId())
      .withContext(context)
      .withOkapiUrl(params.getConnectionUrl())
      .withTenant(params.getTenantId())
      .withToken(params.getToken());
  }

  public Future<Boolean> sendDiErrorEvent(Throwable throwable, ConnectionParams okapiParams, String jobExecutionId,
                                          String tenantId, Record currentRecord) {
    log.debug("sendDiErrorEvent:: Sending DI_ERROR event for jobExecutionId: {} and recordId: {}", jobExecutionId, currentRecord.getId(), throwable);
      okapiParams.getHeaders().put(RECORD_ID_HEADER, currentRecord.getId());
      for (DiErrorPayloadBuilder payloadBuilder: errorPayloadBuilders) {
        if (payloadBuilder.isEligible(currentRecord.getRecordType())) {
          log.info("sendDiErrorEvent:: Start building DI_ERROR payload for jobExecutionId {} and recordId {}", jobExecutionId, currentRecord.getId());
          return payloadBuilder.buildEventPayload(throwable, okapiParams, jobExecutionId, currentRecord)
            .compose(payload -> EventHandlingUtil.sendEventToKafka(tenantId, Json.encode(payload), DI_ERROR.value(),
              KafkaHeaderUtils.kafkaHeadersFromMap(okapiParams.getHeaders()), kafkaConfig, null));
        }
      }
      log.warn("sendDiErrorEvent:: Appropriate DI_ERROR payload builder not found, DI_ERROR without records info will be send for jobExecutionId: {} recordId: {}", jobExecutionId, currentRecord.getId());
      sendDiError(throwable, jobExecutionId, okapiParams, currentRecord);
      return Future.succeededFuture(true);
  }

  private void sendDiError(Throwable throwable, String jobExecutionId, ConnectionParams okapiParams, Record record) {
    HashMap<String, String> context = new HashMap<>();
    context.put(ERROR_KEY, throwable.getMessage());
    if (record != null) {
      context.put(INCOMING_RECORD_ID, record.getId());
      if (record.getRecordType() != null) {
        context.put(RecordConversionUtil.getEntityType(record).value(), Json.encode(record));
      }
    }

    DataImportEventPayload payload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withJobExecutionId(jobExecutionId)
      .withOkapiUrl(okapiParams.getConnectionUrl())
      .withTenant(okapiParams.getTenantId())
      .withToken(okapiParams.getToken())
      .withContext(context);
    EventHandlingUtil.sendEventToKafka(okapiParams.getTenantId(), Json.encode(payload), DI_ERROR.value(),
      KafkaHeaderUtils.kafkaHeadersFromMap(okapiParams.getHeaders()), kafkaConfig, null);
  }
}
