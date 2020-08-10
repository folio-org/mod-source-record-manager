package org.folio.services.util;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.logging.log4j.util.Strings;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
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
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
    OkapiConnectionParams okapiConnectionParams = fromKafkaHeaders(kafkaHeaders);
    String correlationId = okapiConnectionParams.getHeaders().get("correlationId");
    String chunkNumber = okapiConnectionParams.getHeaders().get("chunkNumber");

    Event event = new JsonObject(record.value()).mapTo(Event.class);

    try {
      RecordsBatchResponse recordsBatchResponse = new JsonObject(ZIPArchiver.unzip(event.getEventPayload())).mapTo(RecordsBatchResponse.class);
      List<Record> storedRecords = recordsBatchResponse.getRecords();

      LOGGER.debug("RecordsBatchResponse has been received, starting processing correlationId:" + correlationId + " chunkNumber:" + chunkNumber);
      return eventDrivenChunkProcessingService.sendEventsWithStoredRecords(storedRecords, okapiConnectionParams.getHeaders().get("jobExecutionId"), okapiConnectionParams)
        .compose(b -> {
          LOGGER.debug("RecordsBatchResponse processing has been completed correlationId:" + correlationId + " chunkNumber:" + chunkNumber);
          return Future.succeededFuture(correlationId);
        }, th -> {
          th.printStackTrace();
          LOGGER.error("RecordsBatchResponse processing has failed with errors correlationId:" + correlationId + " chunkNumber:" + chunkNumber);
          return Future.failedFuture(th);
        });
    } catch (IOException e) {
      e.printStackTrace();
      LOGGER.error("Can't process the kafka record: ", e);
      return Future.failedFuture(e);
    }
  }

  //TODO: utility method must be moved out from here
  private OkapiConnectionParams fromKafkaHeaders(List<KafkaHeader> headers) {
    Map<String, String> okapiHeaders = headers
      .stream()
      .collect(Collectors.groupingBy(KafkaHeader::key,
        Collectors.reducing(Strings.EMPTY,
          header -> {
            Buffer value = header.value();
            return Objects.isNull(value) ? "" : value.toString();
          },
          (a, b) -> Strings.isNotBlank(a) ? a : b)));

    return new OkapiConnectionParams(okapiHeaders, vertx);
  }

}
