package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.services.ChunkProcessingService;
import org.folio.services.EventProcessedService;
import org.folio.services.exceptions.RawChunkRecordsParsingException;
import org.folio.services.exceptions.RecordsPublishingException;
import org.folio.services.flowcontrol.RawRecordsFlowControlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Qualifier("RawMarcChunksKafkaHandler")
public class RawMarcChunksKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger();

  private final ChunkProcessingService eventDrivenChunkProcessingService;
  private final EventProcessedService eventProcessedService;
  private final RawRecordsFlowControlService flowControlService;
  private final Vertx vertx;

  public RawMarcChunksKafkaHandler(@Autowired @Qualifier("eventDrivenChunkProcessingService")
                                     ChunkProcessingService eventDrivenChunkProcessingService,
                                   @Autowired EventProcessedService eventProcessedService,
                                   @Autowired RawRecordsFlowControlService flowControlService,
                                   @Autowired Vertx vertx) {
    this.eventDrivenChunkProcessingService = eventDrivenChunkProcessingService;
    this.eventProcessedService = eventProcessedService;
    this.flowControlService = flowControlService;
    this.vertx = vertx;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
    String chunkId = okapiParams.getHeaders().get("chunkId");
    String chunkNumber = okapiParams.getHeaders().get("chunkNumber");
    String jobExecutionId = okapiParams.getHeaders().get("jobExecutionId");

    Event event = Json.decodeValue(record.value(), Event.class);
    LOGGER.debug("Starting to handle of raw mark chunks from Kafka for event type: {}", event.getEventType());
    try {
      RawRecordsDto rawRecordsDto = new JsonObject(event.getEventPayload()).mapTo(RawRecordsDto.class);
      if (!rawRecordsDto.getRecordsMetadata().getLast()) {
        eventProcessedService.increaseEventsToProcess(rawRecordsDto.getInitialRecords().size(), okapiParams.getTenantId())
          .onSuccess(counterValue -> flowControlService.trackChunkReceivedEvent(okapiParams.getTenantId(), counterValue));
      }

      LOGGER.debug("RawRecordsDto has been received, starting processing jobExecutionId: {} chunkId: {} chunkNumber: {} - {}",
        jobExecutionId, chunkId, chunkNumber, rawRecordsDto.getRecordsMetadata());
      return eventDrivenChunkProcessingService
        .processChunk(rawRecordsDto, jobExecutionId, okapiParams)
        .compose(b -> {
          LOGGER.debug("RawRecordsDto processing has been completed chunkId: {} chunkNumber: {} - {} for jobExecutionId: {}", chunkId, chunkNumber, rawRecordsDto.getRecordsMetadata(), jobExecutionId);
          return Future.succeededFuture(record.key());
        }, th -> {
          if (th instanceof DuplicateEventException) {
            LOGGER.info("Duplicate RawRecordsDto processing has been skipped for chunkId: {} chunkNumber: {} - {} for jobExecutionId: {}", chunkId, chunkNumber, rawRecordsDto.getRecordsMetadata(), jobExecutionId);
            if (!rawRecordsDto.getRecordsMetadata().getLast()) {
              eventProcessedService.decreaseEventsToProcess(rawRecordsDto.getInitialRecords().size(), okapiParams.getTenantId())
                .onSuccess(counterValue -> flowControlService.trackChunkDuplicateEvent(okapiParams.getTenantId(), counterValue));
            }
            return Future.failedFuture(th);
          } else if (th instanceof RecordsPublishingException) {
            LOGGER.error("RawRecordsDto entries publishing to Kafka has failed for chunkId: {} chunkNumber: {} - {} for jobExecutionId: {}", chunkId, chunkNumber, rawRecordsDto.getRecordsMetadata(), jobExecutionId, th);
            return Future.failedFuture(th);
          } else {
            LOGGER.error("RawRecordsDto processing has failed with errors chunkId: {} chunkNumber: {} - {} for jobExecutionId: {}", chunkId, chunkNumber, rawRecordsDto.getRecordsMetadata(), jobExecutionId, th);
            return Future.failedFuture(new RawChunkRecordsParsingException(th, rawRecordsDto));
          }
        });
    } catch (Exception e) {
      LOGGER.error("Can't process kafka record: ", e);
      return Future.failedFuture(e);
    }
  }
}
