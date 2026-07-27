package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import lombok.extern.log4j.Log4j2;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.rest.jaxrs.model.DataImportInitConfig;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.JobExecutionService;
import org.folio.services.progress.JobExecutionProgressService;
import org.springframework.stereotype.Component;

import java.util.List;

@Log4j2
@Component
public class DataImportInitKafkaHandler implements AsyncRecordHandler<String, String> {

  private final JobExecutionService jobExecutionService;
  private final JobExecutionProgressService jobExecutionProgressService;

  public DataImportInitKafkaHandler(JobExecutionProgressService jobExecutionProgressService,
                                    JobExecutionService jobExecutionService) {
    this.jobExecutionProgressService = jobExecutionProgressService;
    this.jobExecutionService = jobExecutionService;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    try {
      List<KafkaHeader> kafkaHeaders = record.headers();
      ConnectionParams okapiParams = ConnectionParams.createSystemUserConnectionParams(
        KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders));
      Event event = Json.decodeValue(record.value(), Event.class);
      DataImportInitConfig initConfig = Json.decodeValue(event.getEventPayload(), DataImportInitConfig.class);

      return jobExecutionProgressService.initializeJobExecutionProgress(initConfig.getJobExecutionId(), initConfig.getTotalRecords(), okapiParams.getTenantId())
        .compose(p -> checkAndUpdateToInProgressState(initConfig.getJobExecutionId(), okapiParams))
        .compose(p -> Future.succeededFuture(record.key()));
    } catch (Exception e) {
      log.warn("handle:: Error during processing event for import job progress initialization", e);
      return Future.failedFuture(e);
    }
  }

  private Future<JobExecution> checkAndUpdateToInProgressState(String jobExecutionId, ConnectionParams params) {
    return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
      .compose(jobExecutionOptional -> {
        if (jobExecutionOptional.isPresent()) {
          JobExecution jobExecution = jobExecutionOptional.get();
          if (jobExecution.getStatus() == JobExecution.Status.FILE_UPLOADED) {
            log.info("checkAndUpdateToInProgressState:: Moving from file uploaded to in progress state for jobExecutionId: {}", jobExecutionId);
            StatusDto statusDto = new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS);
            return jobExecutionService.updateJobExecutionStatus(jobExecutionId, statusDto, params);
          }
        }
        return Future.succeededFuture();
      });
  }
}
