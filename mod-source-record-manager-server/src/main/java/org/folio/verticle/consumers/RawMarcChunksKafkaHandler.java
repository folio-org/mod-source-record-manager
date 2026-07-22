package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.impl.future.FailedFuture;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import lombok.extern.log4j.Log4j2;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.ChunkProcessingService;
import org.folio.services.exceptions.InvalidJobProfileForFileException;
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

@Log4j2
@Component
@Qualifier("RawMarcChunksKafkaHandler")
public class RawMarcChunksKafkaHandler implements AsyncRecordHandler<String, byte[]> {

  private final ChunkProcessingService eventDrivenChunkProcessingService;
  private final RawRecordsFlowControlService flowControlService;
  private final JobExecutionService jobExecutionService;

  public RawMarcChunksKafkaHandler(@Autowired @Qualifier("eventDrivenChunkProcessingService")
                                   ChunkProcessingService eventDrivenChunkProcessingService,
                                   @Autowired RawRecordsFlowControlService flowControlService,
                                   @Autowired JobExecutionService jobExecutionService) {
    this.eventDrivenChunkProcessingService = eventDrivenChunkProcessingService;
    this.jobExecutionService = jobExecutionService;
    this.flowControlService = flowControlService;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, byte[]> record) {
    List<KafkaHeader> kafkaHeaders = record.headers();
    ConnectionParams okapiParams = ConnectionParams.createSystemUserConnectionParams(
      KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders));
    String chunkId = okapiParams.getHeaders().get("chunkId");
    String chunkNumber = okapiParams.getHeaders().get("chunkNumber");
    String jobExecutionId = okapiParams.getHeaders().get("jobExecutionId");

    return jobExecutionService.getJobExecutionById(jobExecutionId, okapiParams.getTenantId())
      .compose(jobExecutionOptional -> jobExecutionOptional.map(jobExecution -> {
          if(isNeedToSkip(jobExecution)) {
            log.info("handle:: do not handle because jobExecution with id: {} was cancelled", jobExecutionId);
            flowControlService.triggerNextChunksFetch(okapiParams.getTenantId());
            return Future.succeededFuture(record.key());
          }

          try {
            Event event = DatabindCodec.mapper().readValue(record.value(), Event.class);
            log.debug("handle:: Starting to handle of raw mark chunks from Kafka for event type: {} jobExecutionId: {} chunkId: {}", event.getEventType(), jobExecutionId, chunkId);
            RawRecordsDto rawRecordsDto = Json.decodeValue(event.getEventPayload(), RawRecordsDto.class);
            if (!rawRecordsDto.getRecordsMetadata().getLast()) {
              flowControlService.trackChunkReceivedEvent(okapiParams.getTenantId(), rawRecordsDto.getInitialRecords().size());
            }

            log.debug("handle:: RawRecordsDto has been received, starting processing jobExecutionId: {} chunkId: {} chunkNumber: {} - {}",
              jobExecutionId, chunkId, chunkNumber, rawRecordsDto.getRecordsMetadata());

            return eventDrivenChunkProcessingService
              .processChunk(rawRecordsDto, jobExecution, okapiParams)
              .compose(b -> {
                  log.debug("handle:: RawRecordsDto processing has been completed chunkId: {} chunkNumber: {} - {} for jobExecutionId: {}", chunkId, chunkNumber, rawRecordsDto.getRecordsMetadata(), jobExecutionId);
                  return Future.succeededFuture(record.key());
                },
                th -> {
                  if (th instanceof DuplicateEventException) {
                    log.info("handle:: Duplicate RawRecordsDto processing has been skipped for chunkId: {} chunkNumber: {} - {} for jobExecutionId: {}", chunkId, chunkNumber, rawRecordsDto.getRecordsMetadata(), jobExecutionId);
                    if (!rawRecordsDto.getRecordsMetadata().getLast()) {
                      flowControlService.trackChunkDuplicateEvent(okapiParams.getTenantId(), rawRecordsDto.getInitialRecords().size());
                    }
                    return Future.failedFuture(th);
                  } else if (th instanceof RecordsPublishingException) {
                    log.warn("handle:: RawRecordsDto entries publishing to Kafka has failed for chunkId: {} chunkNumber: {} - {} for jobExecutionId: {}", chunkId, chunkNumber, rawRecordsDto.getRecordsMetadata(), jobExecutionId, th);
                    return Future.failedFuture(th);
                  } else if (th instanceof InvalidJobProfileForFileException) {
                    jobExecutionService.updateJobExecutionStatus(jobExecutionId, new StatusDto()
                        .withStatus(StatusDto.Status.ERROR)
                        .withErrorStatus(StatusDto.ErrorStatus.FILE_PROCESSING_ERROR),
                      okapiParams);
                    log.warn("handle:: Invalid job profile selected for uploaded file for chunkId: {} chunkNumber: {} - {} for jobExecutionId: {} chunkNUmber - {}", chunkId, chunkNumber, rawRecordsDto.getRecordsMetadata(), jobExecutionId, chunkNumber);
                    return Future.failedFuture(th);
                  } else {
                    log.warn("handle:: RawRecordsDto processing has failed with errors chunkId: {} chunkNumber: {} - {} for jobExecutionId: {}", chunkId, chunkNumber, rawRecordsDto.getRecordsMetadata(), jobExecutionId, th);
                    return Future.failedFuture(new RawChunkRecordsParsingException(th, rawRecordsDto));
                  }
                });
          } catch (Exception e) {
            log.warn("handle:: Can't process kafka record, jobExecutionId: {}", jobExecutionId, e);
            return new FailedFuture<String>(e);
          }
        })
        .orElse(Future.failedFuture(new NotFoundException(format("Couldn't find JobExecution with id %s chunkId:%s chunkNumber: %s", jobExecutionId, chunkId, chunkNumber)))));
  }
}
