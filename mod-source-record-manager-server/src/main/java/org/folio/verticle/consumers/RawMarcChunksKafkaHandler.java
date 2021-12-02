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
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.services.ChunkProcessingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@Qualifier("RawMarcChunksKafkaHandler")
public class RawMarcChunksKafkaHandler implements AsyncRecordHandler<String, String> {

  private static final Logger LOGGER = LogManager.getLogger();

  private ChunkProcessingService eventDrivenChunkProcessingService;
  private Vertx vertx;

  public RawMarcChunksKafkaHandler(@Autowired @Qualifier("eventDrivenChunkProcessingService")
                                     ChunkProcessingService eventDrivenChunkProcessingService,
                                   @Autowired Vertx vertx) {
    this.eventDrivenChunkProcessingService = eventDrivenChunkProcessingService;
    this.vertx = vertx;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
    String chunkId = okapiConnectionParams.getHeaders().get("chunkId");
    String chunkNumber = okapiConnectionParams.getHeaders().get("chunkNumber");
    String jobExecutionId = okapiConnectionParams.getHeaders().get("jobExecutionId");

    Event event = Json.decodeValue(record.value(), Event.class);
    LOGGER.debug("Starting to handle of raw mark chunks from Kafka for event type: {}", event.getEventType());
    try {
      RawRecordsDto rawRecordsDto = new JsonObject(event.getEventPayload()).mapTo(RawRecordsDto.class);
      LOGGER.debug("RawRecordsDto has been received, starting processing jobExecutionId: {} chunkId: {} chunkNumber: {} - {}",
        jobExecutionId, chunkId, chunkNumber, rawRecordsDto.getRecordsMetadata());
      return eventDrivenChunkProcessingService
        .processChunk(rawRecordsDto, okapiConnectionParams.getHeaders().get("jobExecutionId"), okapiConnectionParams)
        .compose(b -> {
          LOGGER.debug("RawRecordsDto processing has been completed chunkId: {} chunkNumber: {} - {} for jobExecutionId: {}", chunkId, chunkNumber, rawRecordsDto.getRecordsMetadata(), jobExecutionId);
          return Future.succeededFuture(record.key());
        }, th -> {
          LOGGER.error("RawRecordsDto processing has failed with errors chunkId: {} chunkNumber: {} - {} for jobExecutionId: {}", chunkId, chunkNumber, rawRecordsDto.getRecordsMetadata(), jobExecutionId, th);
          return Future.failedFuture(th);
        });
    } catch (Exception e) {
      LOGGER.error("Can't process kafka record: ", e);
      return Future.failedFuture(e);
    }
  }
}
