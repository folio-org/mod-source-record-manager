package org.folio.verticle.consumers.errorhandlers;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.ProcessRecordErrorHandler;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.services.exceptions.RecordsProcessingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.EntityType.*;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_AUTHORITY;
import static org.folio.services.util.EventHandlingUtil.sendEventToKafka;

@Component
@Qualifier("StoredRecordChunksErrorHandler")
public class StoredRecordChunksErrorHandler implements ProcessRecordErrorHandler<String, String> {
  private static final Logger LOGGER = LogManager.getLogger();

  public static final String ERROR_KEY = "ERROR";
  public static final String JOB_EXECUTION_ID_HEADER = "jobExecutionId";
  public static final String RECORD_ID_HEADER = "recordId";
  private static final String CHUNK_NUMBER = "chunkNumber";

  private final Vertx vertx;
  private final KafkaConfig kafkaConfig;

  @Autowired
  public StoredRecordChunksErrorHandler(Vertx vertx, KafkaConfig kafkaConfig) {
    this.vertx = vertx;
    this.kafkaConfig = kafkaConfig;
  }

  @Override
  public void handle(Throwable throwable, KafkaConsumerRecord<String, String> kafkaConsumerRecord) {
    List<KafkaHeader> kafkaHeaders = kafkaConsumerRecord.headers();
    OkapiConnectionParams okapiParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
    String jobExecutionId = okapiParams.getHeaders().get(JOB_EXECUTION_ID_HEADER);


    // process for specific failure processed records from Exception body
    if (throwable instanceof RecordsProcessingException) {
      List<Record> failedRecords = ((RecordsProcessingException) throwable).getFailedRecords();
      for (Record failedRecord: failedRecords) {
        sendDiErrorForRecord(jobExecutionId, failedRecord, okapiParams, failedRecord.getErrorRecord().getDescription());
      }

    } else if (throwable instanceof DuplicateEventException) {
        LOGGER.info(throwable.getMessage());

    } else {
      // process for all other cases that will include all records
      Event event = Json.decodeValue(kafkaConsumerRecord.value(), Event.class);
      RecordsBatchResponse recordCollection = Json.decodeValue(event.getEventPayload(), RecordsBatchResponse.class);
      for (Record targetRecord: recordCollection.getRecords()) {
        sendDiErrorForRecord(jobExecutionId, targetRecord, okapiParams, throwable.getMessage());
      }
    }
  }

  private void sendDiErrorForRecord(String jobExecutionId, Record targetRecord, OkapiConnectionParams okapiParams, String errorMsg) {
    DataImportEventPayload errorPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withJobExecutionId(jobExecutionId)
      .withOkapiUrl(okapiParams.getOkapiUrl())
      .withTenant(okapiParams.getTenantId())
      .withToken(okapiParams.getToken())
      .withContext(new HashMap<>() {{
        put(getSourceRecordKey(targetRecord), Json.encode(targetRecord));
        put(ERROR_KEY, errorMsg);
      }});

    okapiParams.getHeaders().set(RECORD_ID_HEADER, targetRecord.getId());

    String chunkNumber = okapiParams.getHeaders().get(CHUNK_NUMBER);

    sendEventToKafka(okapiParams.getTenantId(), Json.encode(errorPayload), DI_ERROR.value(), KafkaHeaderUtils.kafkaHeadersFromMultiMap(okapiParams.getHeaders()), kafkaConfig, null)
      .onFailure(th -> LOGGER.error("Error publishing DI_ERROR event for jobExecutionId: {} , recordId: {}, chunkNumber: {}", errorPayload.getJobExecutionId(), targetRecord.getId(), chunkNumber, th));
  }

  private String getSourceRecordKey(Record targetRecord) {
    switch (targetRecord.getRecordType()) {
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
}
