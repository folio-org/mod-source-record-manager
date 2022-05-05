package org.folio.services;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.verticle.consumers.consumerstorage.KafkaConsumersStorage;
import org.folio.verticle.consumers.consumerstorage.KafkaConsumersStorageImpl;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_READ;

@RunWith(VertxUnitRunner.class)
public class KafkaConsumersStorageImplTest {

  private KafkaConsumersStorage kafkaConsumersStorage = new KafkaConsumersStorageImpl();

  @Test
  public void shouldAddConsumer() {
    kafkaConsumersStorage.addConsumer(DI_RAW_RECORDS_CHUNK_READ.value(), KafkaConsumerWrapper.<String, String>builder().build());

    assertEquals(1, kafkaConsumersStorage.getConsumersList().size());
  }

  @Test
  public void shouldGetConsumerByName() {
    kafkaConsumersStorage.addConsumer(DI_RAW_RECORDS_CHUNK_READ.value(), KafkaConsumerWrapper.<String, String>builder().build());

    assertNotNull(kafkaConsumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value()));

  }
}
