package org.folio.verticle.consumers.errorhandlers;

import static org.folio.kafka.KafkaHeaderUtils.kafkaHeadersFromMap;
import static org.folio.kafka.KafkaHeaderUtils.kafkaHeadersToMap;
import static org.folio.services.util.EventHandlingUtil.JOB_EXECUTION_ID_HEADER;
import static org.folio.services.util.EventHandlingUtil.RECORD_ID_HEADER;

import io.vertx.core.Vertx;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.ProcessRecordErrorHandler;
import org.folio.rest.jaxrs.model.Event;
import org.folio.services.exceptions.DeduplicationLookupException;
import org.folio.services.util.EventHandlingUtil;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

/**
 * Error handler for DI_COMPLETED and DI_ERROR consumers.
 * The kafka wrapper commits the offset of a failed record, so when the deduplication lookup fails with a
 * database error (the event was not yet processed) the original event is republished according to the
 * {@link RedeliveryPolicy}, otherwise it would never be counted in the job progress.
 * Other failures are only logged: they happen after the deduplication row was written, so redelivery would be skipped.
 */
@Log4j2
@Component
@Qualifier("DataImportErrorHandler")
public class DataImportErrorHandler implements ProcessRecordErrorHandler<String, byte[]> {

  private final Vertx vertx;
  private final KafkaConfig kafkaConfig;
  private final RedeliveryPolicy policy;

  public DataImportErrorHandler(Vertx vertx, KafkaConfig kafkaConfig, RedeliveryPolicy policy) {
    this.vertx = vertx;
    this.kafkaConfig = kafkaConfig;
    this.policy = policy;
  }

  @Override
  public void handle(Throwable throwable, KafkaConsumerRecord<String, byte[]> record) {
    if (!(throwable instanceof DeduplicationLookupException)) {
      log.warn("handle:: Event processing failed and will not be redelivered, topic: {}, offset: {}",
        record.topic(), record.offset(), throwable);
      return;
    }
    try {
      var event = DatabindCodec.mapper().readValue(record.value(), Event.class);
      var headers = kafkaHeadersToMap(record.headers());
      int attemptsMade = policy.attemptsMade(headers);
      if (policy.isExhausted(attemptsMade)) {
        log.error("handle:: Giving up redelivery of event {} of type {} after {} attempts, jobExecutionId: {}, recordId: {}",
          event.getId(), event.getEventType(), attemptsMade, headers.get(JOB_EXECUTION_ID_HEADER), headers.get(RECORD_ID_HEADER));
        return;
      }
      redeliver(event, record.key(), headers, attemptsMade);
    } catch (Exception e) {
      log.error("handle:: Failed to redeliver event, topic: {}, offset: {}", record.topic(), record.offset(), e);
    }
  }

  private void redeliver(Event event, String key, Map<String, String> headers, int attemptsMade) {
    policy.recordAttempt(headers, attemptsMade);
    long delay = policy.delayMs(attemptsMade);
    log.warn("handle:: Redelivering event {} of type {} in {} ms, attempt {}, jobExecutionId: {}, recordId: {}",
      event.getId(), event.getEventType(), delay, attemptsMade + 1, headers.get(JOB_EXECUTION_ID_HEADER), headers.get(RECORD_ID_HEADER));
    // the original event (with its id) is republished, so deduplication still protects against double counting
    vertx.setTimer(delay, timerId -> EventHandlingUtil.sendEventToKafka(event, kafkaHeadersFromMap(headers), kafkaConfig, key)
      .onFailure(e -> log.error("handle:: Failed to republish event {} of type {}, jobExecutionId: {}, recordId: {}",
        event.getId(), event.getEventType(), headers.get(JOB_EXECUTION_ID_HEADER), headers.get(RECORD_ID_HEADER), e)));
  }
}
