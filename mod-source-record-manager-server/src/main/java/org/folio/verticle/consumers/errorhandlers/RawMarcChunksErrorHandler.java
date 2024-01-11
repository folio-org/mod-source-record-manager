package org.folio.verticle.consumers.errorhandlers;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.ProcessRecordErrorHandler;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.exceptions.RawChunkRecordsParsingException;
import org.folio.services.exceptions.RecordsPublishingException;
import org.folio.services.util.EventHandlingUtil;
import org.folio.services.util.RecordConversionUtil;
import org.folio.verticle.consumers.errorhandlers.payloadbuilders.DiErrorPayloadBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;

@Component
@Qualifier("RawMarcChunksErrorHandler")
public class RawMarcChunksErrorHandler implements ProcessRecordErrorHandler<String, byte[]> {
  private static final Logger LOGGER = LogManager.getLogger();

  public static final String ERROR_KEY = "ERROR";
  public static final String JOB_EXECUTION_ID_HEADER = "jobExecutionId";
  public static final String RECORD_ID_HEADER = "recordId";
  public static final String CHUNK_ID_HEADER = "chunkId";
  private static final String LAST_CHUNK_HEADER = "EOF";

  @Autowired
  private Vertx vertx;
  @Autowired
  private KafkaConfig kafkaConfig;
  @Autowired
  private List<DiErrorPayloadBuilder> errorPayloadBuilders;
  @Autowired
  private ParsedRecordsDiErrorProvider parsedRecordsErrorProvider;

  @Override
  public void handle(Throwable throwable, KafkaConsumerRecord<String, byte[]> record) {
      Event event = null;
      try {
          event = DatabindCodec.mapper().readValue(record.value(), Event.class);
      } catch (IOException e) {
        LOGGER.error("Something happened while deserializing kafka record", e);
        return;
      }
      List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
    String jobExecutionId = okapiParams.getHeaders().get(JOB_EXECUTION_ID_HEADER);
    String chunkId = okapiParams.getHeaders().get(CHUNK_ID_HEADER);
    String tenantId = okapiParams.getTenantId();
    String lastChunk = okapiParams.getHeaders().get(LAST_CHUNK_HEADER);

    if (StringUtils.isNotBlank(lastChunk)) {
      LOGGER.warn("handle:: Source chunk with jobExecutionId: {} , tenantId: {}, chunkId: {} marked as last, prevent sending DI error", jobExecutionId, tenantId, chunkId, throwable);
    } else if (throwable instanceof RecordsPublishingException) {
      List<Record> failedRecords = ((RecordsPublishingException) throwable).getFailedRecords();
      for (Record failedRecord: failedRecords) {
        sendDiErrorEvent(throwable, okapiParams, jobExecutionId, tenantId, failedRecord);
      }
    } else if (throwable instanceof DuplicateEventException) {
      RawRecordsDto rawRecordsDto = Json.decodeValue(event.getEventPayload(), RawRecordsDto.class);
      LOGGER.info("handle:: Duplicate event received, skipping parsing for jobExecutionId: {} , tenantId: {}, chunkId:{}, totalRecords: {}, cause: {}", jobExecutionId, tenantId, chunkId, rawRecordsDto.getInitialRecords().size(), throwable.getMessage());
    } else if (throwable instanceof RawChunkRecordsParsingException) {
      RawChunkRecordsParsingException exception = (RawChunkRecordsParsingException) throwable;
      parsedRecordsErrorProvider.getParsedRecordsFromInitialRecords(okapiParams, jobExecutionId, exception.getRawRecordsDto())
        .onComplete(ar -> {
          List<Record> parsedRecords = ar.result();
          if (CollectionUtils.isNotEmpty(parsedRecords)) {
            for (Record rec : parsedRecords) {
              sendDiError(throwable, jobExecutionId, okapiParams, rec);
            }
          } else {
            sendDiError(throwable, jobExecutionId, okapiParams, null);
          }
        });
    }
    else {
      sendDiErrorEvent(throwable, okapiParams, jobExecutionId, tenantId, null);
    }
  }

  private void sendDiErrorEvent(Throwable throwable,
                                OkapiConnectionParams okapiParams,
                                String jobExecutionId,
                                String tenantId,
                                Record record) {
    if (record != null) {
      okapiParams.getHeaders().set(RECORD_ID_HEADER, record.getId());
      for (DiErrorPayloadBuilder payloadBuilder: errorPayloadBuilders) {
        if (payloadBuilder.isEligible(record.getRecordType())) {
          LOGGER.info("sendDiErrorEvent:: Start building DI_ERROR payload for jobExecutionId {} and recordId {}", jobExecutionId, record.getId());
          payloadBuilder.buildEventPayload(throwable, okapiParams, jobExecutionId, record)
            .compose(payload -> EventHandlingUtil.sendEventToKafka(tenantId, Json.encode(payload), DI_ERROR.value(),
              KafkaHeaderUtils.kafkaHeadersFromMultiMap(okapiParams.getHeaders()), kafkaConfig, null));
          return;
        }
      }
      LOGGER.warn("sendDiErrorEvent:: Appropriate DI_ERROR payload builder not found, DI_ERROR without records info will be send");
    }
    sendDiError(throwable, jobExecutionId, okapiParams, null);
  }

  private void sendDiError(Throwable throwable, String jobExecutionId, OkapiConnectionParams okapiParams, Record record) {
    HashMap<String, String> context = new HashMap<>();
    context.put(ERROR_KEY, throwable.getMessage());
    if (record != null && record.getRecordType() != null) {
      context.put(RecordConversionUtil.getEntityType(record).value(), Json.encode(record));
    }

    DataImportEventPayload payload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withJobExecutionId(jobExecutionId)
      .withOkapiUrl(okapiParams.getOkapiUrl())
      .withTenant(okapiParams.getTenantId())
      .withToken(okapiParams.getToken())
      .withContext(context);
    EventHandlingUtil.sendEventToKafka(okapiParams.getTenantId(), Json.encode(payload), DI_ERROR.value(),
      KafkaHeaderUtils.kafkaHeadersFromMultiMap(okapiParams.getHeaders()), kafkaConfig, null);
  }
}
