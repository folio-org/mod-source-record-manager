package org.folio.verticle.consumers.errorhandlers;

import static org.folio.kafka.KafkaHeaderUtils.kafkaHeadersFromMap;
import static org.folio.kafka.KafkaHeaderUtils.kafkaHeadersToMap;

import io.vertx.core.Vertx;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import java.util.Map;
import lombok.extern.log4j.Log4j2;
import org.folio.dataimport.util.ConnectionParams;
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
 * database error (the event was not yet processed) the original event is republished with exponential backoff,
 * otherwise it would never be counted in the job progress.
 * Other failures are only logged: they happen after the deduplication row was written, so redelivery would be skipped.
 */
@Log4j2
@Component
@Qualifier("DataImportErrorHandler")
public class DataImportErrorHandler implements ProcessRecordErrorHandler<String, byte[]> {

  public static final String RETRY_COUNT_HEADER = "diRetryCount";
  static final int MAX_RETRIES = 15;
  static final long BASE_DELAY_MS = 1000;
  static final long MAX_DELAY_MS = 60_000;
  private static final String ID_STRING_MSG = "jobExecutionId: %s, recordId: %s";

  private final Vertx vertx;
  private final KafkaConfig kafkaConfig;

  public DataImportErrorHandler(Vertx vertx, KafkaConfig kafkaConfig) {
    this.vertx = vertx;
    this.kafkaConfig = kafkaConfig;
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
      var eventId = event.getId();
      var eventType = event.getEventType();

      var params = ConnectionParams.createSystemUserConnectionParams(kafkaHeadersToMap(record.headers()));
      var headers = params.getHeaders();

      int retry = parseRetryCount(headers.get(RETRY_COUNT_HEADER));
      if (retry >= MAX_RETRIES) {
        log.error("handle:: Giving up redelivery of event {} of type {} after {} attempts, {}",
          eventId, eventType, retry, getIdString(headers));
        return;
      }
      redeliverEvent(record, headers, retry, eventId, eventType, event);
    } catch (Exception e) {
      log.error("handle:: Failed to redeliver event, topic: {}, offset: {}", record.topic(), record.offset(), e);
    }
  }

  private void redeliverEvent(KafkaConsumerRecord<String, byte[]> record, Map<String, String> headers, int retry,
                              String eventId, String eventType, Event event) {
    headers.put(RETRY_COUNT_HEADER, String.valueOf(retry + 1));
    long delay = delayMs(retry);
    log.warn("handle:: Redelivering event {} of type {} in {} ms, attempt {}, {}",
      eventId, eventType, delay, retry + 1, getIdString(headers));
    // the original event (with its id) is republished, so deduplication still protects against double counting
    vertx.setTimer(delay,
      timerId -> EventHandlingUtil.sendEventToKafka(event, kafkaHeadersFromMap(headers), kafkaConfig, record.key())
        .onFailure(e -> log.error("handle:: Failed to republish event {} of type {}, {}",
          eventId, eventType, getIdString(headers), e)));
  }

  private String getIdString(Map<String, String> headers) {
    return ID_STRING_MSG.formatted(headers.get("jobExecutionId"), headers.get("recordId"));
  }

  /**
   * Exponential backoff: 1s, 2s, 4s ... capped at 60s.
   */
  private long delayMs(int retry) {
    return Math.min(MAX_DELAY_MS, BASE_DELAY_MS << Math.min(retry, 20));
  }

  private static int parseRetryCount(String value) {
    try {
      return value == null ? 0 : Integer.parseInt(value);
    } catch (NumberFormatException e) {
      return 0;
    }
  }
}
