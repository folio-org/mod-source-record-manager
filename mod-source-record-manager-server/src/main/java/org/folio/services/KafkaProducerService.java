package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.services.util.EventHandlingUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

@Service
public class KafkaProducerService {
  private static final Logger LOGGER = LogManager.getLogger();

  private KafkaTemplate<String, String> kafkaTemplate;

  @Autowired
  public KafkaProducerService(KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  /**
   * Prepares and sends event to kafka.
   *
   * @param tenantId     tenant id
   * @param envId        env id
   * @param eventPayload eventPayload in String representation
   * @param eventType    eventType
   * @param kafkaHeaders kafka headers
   * @return completed future with true if event was sent successfully
   */
  public Future<Boolean> sendEvent(String tenantId, String envId, String eventPayload, String eventType,
                                   List<KafkaHeader> kafkaHeaders, String key) {

    Event event;
    try {
      event = createEvent(eventPayload, eventType, tenantId, true);
    } catch (IOException e) {
      LOGGER.error("Failed to construct an event for eventType {}", eventType, e);
      Promise<Boolean> promise = Promise.promise();
      promise.fail(e);
      return promise.future();
    }

    String topicName = createTopicName(eventType, tenantId, envId);

    ProducerRecord<String, String> record = createProducerRecord(event, key, topicName, kafkaHeaders);

    return sendEvent(eventType, record);
  }

  /**
   * Sends event to kafka.
   *
   * @param  eventType event type
   * @param record     producer record
   * @return completed future with true if event was sent successfully
   */
  public Future<Boolean> sendEvent(String eventType, ProducerRecord<String, String> record) {
    LOGGER.debug("Starting to send event to Kafka for eventType: {}", eventType);

    Promise<Boolean> promise = Promise.promise();

    ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);
    future.addCallback(new ListenableFutureCallback<>() {

      @Override
      public void onSuccess(SendResult<String, String> result) {
        Headers headers = result.getProducerRecord().headers();
        LOGGER.info("Event with type: {} and correlationId: {} was sent to kafka", eventType,  getHeaderValue(headers, "correlationId"));
        promise.complete(true);
      }

      @Override
      public void onFailure(Throwable e) {
        LOGGER.error("Next chunk has failed with errors", e);
        promise.fail(e);
      }
    });

    return promise.future();
  }

  private String getHeaderValue(Headers headers, String headerName) {
    return Optional.ofNullable(headers.lastHeader(headerName)).map(header -> new String(header.value())).orElse(null);
  }

  protected String createTopicName(String eventType, String tenantId, String envId) {
    return KafkaTopicNameHelper.formatTopicName(envId, KafkaTopicNameHelper.getDefaultNameSpace(),
      tenantId, eventType);
  }

  protected Event createEvent(String eventPayload, String eventType, String tenantId, boolean isZipped) throws IOException {
    return new Event()
      .withId(UUID.randomUUID().toString())
      .withEventType(eventType)
      .withEventPayload(isZipped ? ZIPArchiver.zip(eventPayload) : eventPayload)
      .withEventMetadata(new EventMetadata()
        .withTenantId(tenantId)
        .withEventTTL(1)
        .withPublishedBy(EventHandlingUtil.constructModuleName()));
  }


  public ProducerRecord<String, String> createProducerRecord(Event event, String key, String topicName, List<KafkaHeader> kafkaHeaders) {
    List<Header> headers = kafkaHeaders.stream()
      .map(header -> new RecordHeader(header.key(), header.value().getBytes()))
      .collect(Collectors.toList());
    return new ProducerRecord<>(topicName,null, key, Json.encode(event), headers);
  }
}
