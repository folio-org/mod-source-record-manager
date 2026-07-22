package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;

import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.rest.jaxrs.model.Event;
import org.folio.services.EventHandlingService;
import org.folio.services.EventProcessedService;
import org.folio.services.flowcontrol.RawRecordsFlowControlService;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

@Log4j2
@Component
@Qualifier("DataImportKafkaHandler")
public class DataImportKafkaHandler implements AsyncRecordHandler<String, byte[]> {

  static final String RECORD_ID_HEADER = "recordId";
  public static final String JOB_EXECUTION_ID_HEADER = "jobExecutionId";
  public static final String DATA_IMPORT_KAFKA_HANDLER_UUID = "6713adda-72ce-11ec-90d6-0242ac120003";

  private final EventHandlingService eventHandlingService;
  private final EventProcessedService eventProcessedService;
  private final RawRecordsFlowControlService flowControlService;

  public DataImportKafkaHandler(EventHandlingService eventHandlingService,
                                EventProcessedService eventProcessedService,
                                RawRecordsFlowControlService flowControlService) {
    this.eventHandlingService = eventHandlingService;
    this.eventProcessedService = eventProcessedService;
    this.flowControlService = flowControlService;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, byte[]> record) {
    try {
      Promise<String> result = Promise.promise();
      List<KafkaHeader> kafkaHeaders = record.headers();
      ConnectionParams okapiConnectionParams = ConnectionParams.createSystemUserConnectionParams(
        KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders));
      String recordId = okapiConnectionParams.getHeaders().get(RECORD_ID_HEADER);
      Event event = DatabindCodec.mapper().readValue(record.value(), Event.class);
      String jobExecutionId = extractJobExecutionId(kafkaHeaders);
      log.info("handle:: Event was received with recordId: '{}' event type: '{}' with jobExecutionId: '{}'", recordId, event.getEventType(), jobExecutionId);

      if (StringUtils.isBlank(recordId)) {
        handleLocalEvent(result, okapiConnectionParams, event);
        return result.future();
      }

      eventProcessedService.collectData(DATA_IMPORT_KAFKA_HANDLER_UUID, event.getId(), okapiConnectionParams.getTenantId())
        .onSuccess(res -> {
          flowControlService.trackRecordCompleteEvent(okapiConnectionParams.getTenantId(), 1);
          handleLocalEvent(result, okapiConnectionParams, event);
        })
        .onFailure(e -> {
          if (e instanceof DuplicateEventException) {
            log.info("handle:: {} jobExecutionId: {} recordId: {}", e.getMessage(), jobExecutionId, recordId);
            result.complete();
          } else {
            log.warn("handle:: Error with database during collecting of deduplication info for handlerId: {} eventId: {} jobExecutionId: {} recordId: {}",
              DATA_IMPORT_KAFKA_HANDLER_UUID, event.getId(), jobExecutionId, recordId, e);
            result.fail(e);
          }
        });
      return result.future();
    } catch (Exception e) {
      log.warn("handle:: Error during processing data-import result", e);
      return Future.failedFuture(e);
    }
  }

  private void handleLocalEvent(Promise<String> result, ConnectionParams okapiConnectionParams, Event event) {
    eventHandlingService.handle(event.getEventPayload(), okapiConnectionParams)
      .onSuccess(ar -> result.complete())
      .onFailure(e -> {
        log.warn("handleLocalEvent:: Error during processing DataImport Result: ", e);
        result.fail(e);
      });
  }

  private String extractJobExecutionId(List<KafkaHeader> headers) {
    return headers.stream()
      .filter(header -> header.key().equals(JOB_EXECUTION_ID_HEADER))
      .findFirst()
      .map(header -> header.value().toString())
      .orElse(null);
  }
}
