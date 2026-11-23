package org.folio.verticle.consumers.errorhandlers;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;

import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;

/**
 * Builds a consumed kafka record that carries a data-import {@link Event}.
 */
final class DiEventRecordBuilder {

  static final String EVENT_ID = "c9d09a5e-73ba-11ec-90d6-0242ac120003";
  static final String TENANT = "diku";

  private final ConsumerRecord<String, byte[]> record;

  private DiEventRecordBuilder() {
    var event = new Event()
      .withId(EVENT_ID)
      .withEventType(DI_ERROR.value())
      .withEventPayload("{}")
      .withEventMetadata(new EventMetadata().withTenantId(TENANT));
    record = new ConsumerRecord<>("topic", 0, 0, "1", Json.encode(event).getBytes(StandardCharsets.UTF_8));
    withHeader(XOkapiHeaders.TENANT, TENANT);
  }

  static DiEventRecordBuilder aDiEventRecord() {
    return new DiEventRecordBuilder();
  }

  DiEventRecordBuilder withHeader(String name, String value) {
    record.headers().add(new RecordHeader(name, value.getBytes(StandardCharsets.UTF_8)));
    return this;
  }

  KafkaConsumerRecordImpl<String, byte[]> build() {
    return new KafkaConsumerRecordImpl<>(record);
  }
}
