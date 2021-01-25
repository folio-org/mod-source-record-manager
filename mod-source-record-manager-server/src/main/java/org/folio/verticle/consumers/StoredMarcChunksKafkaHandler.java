package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.services.ChunkProcessingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

@Component
@Qualifier("StoredMarcChunksKafkaHandler")
public class StoredMarcChunksKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LoggerFactory.getLogger(StoredMarcChunksKafkaHandler.class);

  private ChunkProcessingService eventDrivenChunkProcessingService;
  private Vertx vertx;

  public StoredMarcChunksKafkaHandler(@Autowired @Qualifier("eventDrivenChunkProcessingService") ChunkProcessingService eventDrivenChunkProcessingService,
                                      @Autowired Vertx vertx) {
    this.eventDrivenChunkProcessingService = eventDrivenChunkProcessingService;
    this.vertx = vertx;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
    String correlationId = okapiConnectionParams.getHeaders().get("correlationId");
    String chunkNumber = okapiConnectionParams.getHeaders().get("chunkNumber");

    Event event = new JsonObject(record.value()).mapTo(Event.class);

    try {
      String unzipped = ZIPArchiver.unzip(event.getEventPayload());
      LOGGER.info("Received stored records payload {}", unzipped);
      RecordsBatchResponse recordsBatchResponse = new JsonObject(unzipped).mapTo(RecordsBatchResponse.class);
      LOGGER.info("Received stored records {}", recordsBatchResponse);
      List<Record> storedRecords = recordsBatchResponse.getRecords();

      LOGGER.debug("RecordsBatchResponse has been received, starting processing correlationId: {} chunkNumber: {}", correlationId, chunkNumber);
      return eventDrivenChunkProcessingService.sendEventsWithStoredRecords(storedRecords, okapiConnectionParams.getHeaders().get("jobExecutionId"), okapiConnectionParams)
        .compose(b -> {
          LOGGER.debug("RecordsBatchResponse processing has been completed correlationId: {} chunkNumber: {}",correlationId, chunkNumber);
          return Future.succeededFuture(correlationId);
        }, th -> {
          LOGGER.error("RecordsBatchResponse processing has failed with errors correlationId: {} chunkNumber: {}", th, correlationId, chunkNumber);
          return Future.failedFuture(th);
        });
    } catch (IOException e) {
      LOGGER.error("Can't process the kafka record: ", e);
      return Future.failedFuture(e);
    }
  }

}
