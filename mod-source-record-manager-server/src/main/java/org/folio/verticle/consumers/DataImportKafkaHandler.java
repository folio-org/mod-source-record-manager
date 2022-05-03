package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.rest.jaxrs.model.Event;
import org.folio.services.EventHandlingService;
import org.folio.services.EventProcessedService;
import org.folio.services.flowcontrol.RawRecordsFlowControlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Qualifier("DataImportKafkaHandler")
public class DataImportKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LogManager.getLogger();
  static final String RECORD_ID_HEADER = "recordId";
  public static final String DATA_IMPORT_KAFKA_HANDLER_UUID = "6713adda-72ce-11ec-90d6-0242ac120003";

  private Vertx vertx;
  private EventHandlingService eventHandlingService;
  private EventProcessedService eventProcessedService;
  private RawRecordsFlowControlService flowControlService;

  public DataImportKafkaHandler(@Autowired Vertx vertx,
                                @Autowired EventHandlingService eventHandlingService,
                                @Autowired EventProcessedService eventProcessedService,
                                @Autowired RawRecordsFlowControlService flowControlService) {
    this.vertx = vertx;
    this.eventHandlingService = eventHandlingService;
    this.eventProcessedService = eventProcessedService;
    this.flowControlService = flowControlService;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    try {
      Promise<String> result = Promise.promise();
      List<KafkaHeader> kafkaHeaders = record.headers();
      OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
      String recordId = okapiConnectionParams.getHeaders().get(RECORD_ID_HEADER);
      Event event = Json.decodeValue(record.value(), Event.class);
      String jobExecutionId = extractJobExecutionId(kafkaHeaders);
      LOGGER.info("Event was received with recordId: '{}' event type: '{}' with jobExecutionId: '{}'", recordId, event.getEventType(), jobExecutionId);

      if (StringUtils.isBlank(recordId)) {
        handleLocalEvent(result, okapiConnectionParams, event);
        return result.future();
      }

      eventProcessedService.collectData(DATA_IMPORT_KAFKA_HANDLER_UUID, event.getId(), okapiConnectionParams.getTenantId())
        .onSuccess(res -> {
          handleLocalEvent(result, okapiConnectionParams, event);
        })
        .onFailure(e -> {
          if (e instanceof DuplicateEventException) {
            LOGGER.info(e.getMessage());
            result.complete();
          } else {
            LOGGER.error("Error with database during collecting of deduplication info for handlerId: {} , eventId: {}. ", DATA_IMPORT_KAFKA_HANDLER_UUID, event.getId(), e);
            result.fail(e);
          }
        });
      return result.future();
    } catch (Exception e) {
      LOGGER.error("Error during processing data-import result", e);
      return Future.failedFuture(e);
    }
  }

  private void handleLocalEvent(Promise<String> result, OkapiConnectionParams okapiConnectionParams, Event event) {
    flowControlService.trackRecordCompleteEvent();

    eventHandlingService.handle(event.getEventPayload(), okapiConnectionParams)
      .onSuccess(ar -> result.complete())
      .onFailure(ar -> {
        LOGGER.error("Error during processing DataImport Result: ", ar.getCause());
        result.fail(ar.getCause());
      });
  }

  private String extractJobExecutionId(List<KafkaHeader> headers) {
    return headers.stream()
      .filter(header -> header.key().equals(RECORD_ID_HEADER))
      .findFirst()
      .map(header -> header.value().toString())
      .orElse(null);
  }
}
