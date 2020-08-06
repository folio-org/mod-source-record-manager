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
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.services.ChunkProcessingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Component
@Qualifier("RawMarcChunksKafkaHandler")
public class RawMarcChunksKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RawMarcChunksKafkaHandler.class);

  private ChunkProcessingService eventDrivenChunkProcessingService;

  private Vertx vertx;


  public RawMarcChunksKafkaHandler(@Autowired @Qualifier("eventDrivenChunkProcessingService") ChunkProcessingService eventDrivenChunkProcessingService,
                                   @Autowired Vertx vertx) {
    super();
    this.eventDrivenChunkProcessingService = eventDrivenChunkProcessingService;
    this.vertx = vertx;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    Event event = new JsonObject(record.value()).mapTo(Event.class);

    try {
      RawRecordsDto rawRecordsDto = new JsonObject(ZIPArchiver.unzip(event.getEventPayload())).mapTo(RawRecordsDto.class);

      List<KafkaHeader> kafkaHeaders = record.headers();
      OkapiConnectionParams okapiConnectionParams = fromKafkaHeaders(kafkaHeaders);

      LOGGER.debug("RawRecordsDto has been received, starting processing... " + rawRecordsDto.getRecordsMetadata());
      return eventDrivenChunkProcessingService
        .processChunk(rawRecordsDto, okapiConnectionParams.getHeaders().get("id"), okapiConnectionParams)
        .compose(b -> {
          LOGGER.debug("RawRecordsDto processing has been completed... " + rawRecordsDto.getRecordsMetadata());
          return Future.succeededFuture(record.key());
        }, th -> {
          th.printStackTrace();
          LOGGER.error("RawRecordsDto processing has failed with errors... " + rawRecordsDto.getRecordsMetadata(), th);
          return Future.failedFuture(th);
        });

    } catch (IOException e) {
      e.printStackTrace();
      LOGGER.error("Can't process the record: ", e);
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
