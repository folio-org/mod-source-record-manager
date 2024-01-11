package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.impl.future.FailedFuture;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
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
import org.folio.services.JobExecutionService;
import org.folio.services.exceptions.RawChunkRecordsParsingException;
import org.folio.services.exceptions.RecordsPublishingException;
import org.folio.services.flowcontrol.RawRecordsFlowControlService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import javax.ws.rs.NotFoundException;
import java.util.List;

import static java.lang.String.format;
import static org.folio.verticle.consumers.util.JobExecutionUtils.isNeedToSkip;

@Component
@Qualifier("RawMarcChunksKafkaHandler")
public class RawMarcChunksKafkaHandler implements AsyncRecordHandler<String, byte[]> {

  private static final Logger LOGGER = LogManager.getLogger();

  private final ChunkProcessingService eventDrivenChunkProcessingService;
  private final RawRecordsFlowControlService flowControlService;
  private final JobExecutionService jobExecutionService;
  private final Vertx vertx;

  public RawMarcChunksKafkaHandler(@Autowired @Qualifier("eventDrivenChunkProcessingService")
                                     ChunkProcessingService eventDrivenChunkProcessingService,
                                   @Autowired RawRecordsFlowControlService flowControlService,
                                   @Autowired JobExecutionService jobExecutionService,
                                   @Autowired Vertx vertx) {
    this.eventDrivenChunkProcessingService = eventDrivenChunkProcessingService;
    this.jobExecutionService = jobExecutionService;
    this.flowControlService = flowControlService;
    this.vertx = vertx;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, byte[]> record) {
    List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
    String chunkId = okapiParams.getHeaders().get("chunkId");
    String chunkNumber = okapiParams.getHeaders().get("chunkNumber");
    String jobExecutionId = okapiParams.getHeaders().get("jobExecutionId");

    return jobExecutionService.getJobExecutionById(jobExecutionId, okapiParams.getTenantId())
      .compose(jobExecutionOptional -> jobExecutionOptional.map(jobExecution -> {
          if(isNeedToSkip(jobExecution)) {
            LOGGER.info("handle:: do not handle because jobExecution with id: {} was cancelled", jobExecutionId);
            return Future.succeededFuture(record.key());
          }

          try {
            Event event = DatabindCodec.mapper().readValue(record.value(), Event.class);
            LOGGER.debug("handle:: Starting to handle of raw mark chunks from Kafka for event type: {}", event.getEventType());
            RawRecordsDto rawRecordsDto = Json.decodeValue(event.getEventPayload(), RawRecordsDto.class);
            if (!rawRecordsDto.getRecordsMetadata().getLast()) {
              flowControlService.trackChunkReceivedEvent(okapiParams.getTenantId(), rawRecordsDto.getInitialRecords().size());
            }

            LOGGER.debug("handle:: RawRecordsDto has been received, starting processing jobExecutionId: {} chunkId: {} chunkNumber: {} - {}",
              jobExecutionId, chunkId, chunkNumber, rawRecordsDto.getRecordsMetadata());

            return eventDrivenChunkProcessingService
              .processChunk(rawRecordsDto, jobExecution, okapiParams)
              .compose(b -> {
                  LOGGER.debug("handle:: RawRecordsDto processing has been completed chunkId: {} chunkNumber: {} - {} for jobExecutionId: {}", chunkId, chunkNumber, rawRecordsDto.getRecordsMetadata(), jobExecutionId);
                  return Future.succeededFuture(record.key());
                },
                th -> {
                  if (th instanceof DuplicateEventException) {
                    LOGGER.info("handle:: Duplicate RawRecordsDto processing has been skipped for chunkId: {} chunkNumber: {} - {} for jobExecutionId: {}", chunkId, chunkNumber, rawRecordsDto.getRecordsMetadata(), jobExecutionId);
                    if (!rawRecordsDto.getRecordsMetadata().getLast()) {
                      flowControlService.trackChunkDuplicateEvent(okapiParams.getTenantId(), rawRecordsDto.getInitialRecords().size());
                    }
                    return Future.failedFuture(th);
                  } else if (th instanceof RecordsPublishingException) {
                    LOGGER.warn("handle:: RawRecordsDto entries publishing to Kafka has failed for chunkId: {} chunkNumber: {} - {} for jobExecutionId: {}", chunkId, chunkNumber, rawRecordsDto.getRecordsMetadata(), jobExecutionId, th);
                    return Future.failedFuture(th);
                  } else {
                    LOGGER.warn("handle:: RawRecordsDto processing has failed with errors chunkId: {} chunkNumber: {} - {} for jobExecutionId: {}", chunkId, chunkNumber, rawRecordsDto.getRecordsMetadata(), jobExecutionId, th);
                    return Future.failedFuture(new RawChunkRecordsParsingException(th, rawRecordsDto));
                  }
                });
          } catch (Exception e) {
            LOGGER.warn("handle:: Can't process kafka record, jobExecutionId: {}", jobExecutionId, e);
            return new FailedFuture<String>(e);
          }
        })
        .orElse(Future.failedFuture(new NotFoundException(format("Couldn't find JobExecution with id %s chunkId:%s chunkNumber: %s", jobExecutionId, chunkId, chunkNumber)))));
  }
}
