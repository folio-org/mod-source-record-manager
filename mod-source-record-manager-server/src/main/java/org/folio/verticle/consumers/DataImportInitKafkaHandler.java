package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.rest.jaxrs.model.DataImportInitConfig;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.services.JobExecutionService;
import org.folio.services.progress.JobExecutionProgressService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class DataImportInitKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LogManager.getLogger();

  private Vertx vertx;
  private JobExecutionService jobExecutionService;
  private JobExecutionProgressService jobExecutionProgressService;

  public DataImportInitKafkaHandler(@Autowired Vertx vertx,
                                    @Autowired JobExecutionProgressService jobExecutionProgressService,
                                    @Autowired JobExecutionService jobExecutionService) {
    this.vertx = vertx;
    this.jobExecutionProgressService = jobExecutionProgressService;
    this.jobExecutionService = jobExecutionService;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
    Event event = Json.decodeValue(record.value(), Event.class);
    DataImportInitConfig initConfig = Json.decodeValue(event.getEventPayload(), DataImportInitConfig.class);

    return jobExecutionProgressService.initializeJobExecutionProgress(initConfig.getJobExecutionId(), initConfig.getTotalRecords(), okapiParams.getTenantId())
      .compose(p -> checkAndUpdateToInProgressState(initConfig.getJobExecutionId(), okapiParams))
      .compose(p -> Future.succeededFuture(record.key()));
  }

  private Future<JobExecution> checkAndUpdateToInProgressState(String jobExecutionId, OkapiConnectionParams params) {
    return jobExecutionService.getJobExecutionById(jobExecutionId, params.getTenantId())
      .compose(jobExecutionOptional -> {
        if (jobExecutionOptional.isPresent()) {
          JobExecution jobExecution = jobExecutionOptional.get();
          if (jobExecution.getStatus() == JobExecution.Status.FILE_UPLOADED) {
            LOGGER.info("Moving from file uploaded to in progress state for jobExecutionId: {}", jobExecutionId);
            StatusDto statusDto = new StatusDto().withStatus(StatusDto.Status.PARSING_IN_PROGRESS);
            return jobExecutionService.updateJobExecutionStatus(jobExecutionId, statusDto, params);
          }
        }
        return Future.succeededFuture();
      });
  }
}
