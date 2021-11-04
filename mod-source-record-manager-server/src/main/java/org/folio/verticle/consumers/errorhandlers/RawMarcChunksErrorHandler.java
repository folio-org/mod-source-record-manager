package org.folio.verticle.consumers.errorhandlers;

import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.ProcessRecordErrorHandler;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.services.util.EventHandlingUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;

@Component
@Qualifier("RawMarcChunksErrorHandler")
public class RawMarcChunksErrorHandler implements ProcessRecordErrorHandler<String, String> {
  private static final Logger LOGGER = LogManager.getLogger();

  public static final String ERROR_KEY = "ERROR";
  public static final String JOB_EXECUTION_ID_HEADER = "jobExecutionId";
  public static final String RECORD_ID_HEADER = "recordId";

  @Autowired
  private Vertx vertx;
  @Autowired
  private KafkaConfig kafkaConfig;

  @Override
  public void handle(Throwable throwable, KafkaConsumerRecord<String, String> record) {
    List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
    String jobExecutionId = okapiParams.getHeaders().get(JOB_EXECUTION_ID_HEADER);
    String tenantId = okapiParams.getTenantId();

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withJobExecutionId(jobExecutionId)
      .withOkapiUrl(okapiParams.getOkapiUrl())
      .withTenant(okapiParams.getTenantId())
      .withToken(okapiParams.getToken())
      .withContext(new HashMap<>(){{
        put(ERROR_KEY, throwable.getMessage());
      }});

    sendDiErrorEvent(eventPayload, okapiParams, jobExecutionId, tenantId);
  }

  private void sendDiErrorEvent(DataImportEventPayload eventPayload, OkapiConnectionParams okapiParams, String jobExecutionId, String tenantId) {

    // recordId is not yet created at this stage of parsing raw marc chunks, so setting random one to not fail calculation of progress bar
    String recordId = UUID.randomUUID().toString();

    okapiParams.getHeaders().set(RECORD_ID_HEADER, recordId);

    EventHandlingUtil.sendEventToKafka(tenantId, Json.encode(eventPayload), DI_ERROR.value(), KafkaHeaderUtils.kafkaHeadersFromMultiMap(okapiParams.getHeaders()), kafkaConfig, null)
     .onFailure(th -> LOGGER.error("Error publishing DI_ERROR event for jobExecutionId: {}", jobExecutionId, th));
  }
}
