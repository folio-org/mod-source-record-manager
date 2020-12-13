package org.folio.services.util;

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
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.services.ChunkProcessingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.List;

@Component
@Qualifier("RawMarcChunksKafkaHandler")
public class RawMarcChunksKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RawMarcChunksKafkaHandler.class);

  private ChunkProcessingService eventDrivenChunkProcessingService;
  private ChunkProcessingService restChunkProcessingService;
  private Vertx vertx;

  public RawMarcChunksKafkaHandler(@Autowired
                                   @Qualifier("eventDrivenChunkProcessingService")
                                     ChunkProcessingService eventDrivenChunkProcessingService,
                                   @Autowired
                                   @Qualifier("restChunkProcessingService")
                                     ChunkProcessingService restChunkProcessingService,
                                   @Autowired Vertx vertx) {
    super();
    this.eventDrivenChunkProcessingService = eventDrivenChunkProcessingService;
    this.restChunkProcessingService = restChunkProcessingService;
    this.vertx = vertx;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
    String correlationId = okapiConnectionParams.getHeaders().get("correlationId");
    String chunkNumber = okapiConnectionParams.getHeaders().get("chunkNumber");
    boolean defaultMapping = Boolean.parseBoolean(okapiConnectionParams.getHeaders().get("defaultMapping"));

    Event event = new JsonObject(record.value()).mapTo(Event.class);

    try {
      RawRecordsDto rawRecordsDto = new JsonObject(ZIPArchiver.unzip(event.getEventPayload())).mapTo(RawRecordsDto.class);
      LOGGER.debug("RawRecordsDto has been received, starting processing correlationId:" + correlationId + " chunkNumber:" + chunkNumber + " - " + rawRecordsDto.getRecordsMetadata());
      ChunkProcessingService chunkProcessingService = defaultMapping ? restChunkProcessingService : eventDrivenChunkProcessingService;
      return chunkProcessingService
        .processChunk(rawRecordsDto, okapiConnectionParams.getHeaders().get("jobExecutionId"), okapiConnectionParams)
        .compose(b -> {
          LOGGER.debug("RawRecordsDto processing has been completed correlationId:" + correlationId + " chunkNumber:" + chunkNumber + " - " + rawRecordsDto.getRecordsMetadata());
          return Future.succeededFuture(record.key());
        }, th -> {
          LOGGER.error("RawRecordsDto processing has failed with errors correlationId:" + correlationId + " chunkNumber:" + chunkNumber + " - " + rawRecordsDto.getRecordsMetadata(), th);
          return Future.failedFuture(th);
        });

    } catch (IOException e) {
      LOGGER.error("Can't process the kafka record: ", e);
      return Future.failedFuture(e);
    }
  }
}
