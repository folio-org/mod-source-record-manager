package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.services.RecordsPublishingService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class StoredRecordChunksKafkaHandlerTest {

  @Mock
  private RecordsPublishingService recordsPublishingService;
  @Mock
  private KafkaInternalCache kafkaInternalCache;
  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;

  private Vertx vertx = Vertx.vertx();
  private AsyncRecordHandler<String, String> storedRecordChunksKafkaHandler;

  @Before
  public void setUp() {
    storedRecordChunksKafkaHandler = new StoredRecordChunksKafkaHandler(recordsPublishingService, kafkaInternalCache, vertx);
  }

  @Test
  public void shouldNotHandleEventWhenKafkaCacheContainsEventId() throws IOException {
    // given
    RecordsBatchResponse recordsBatch = new RecordsBatchResponse()
      .withRecords(List.of(new Record()))
      .withTotalRecords(1);

    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(ZIPArchiver.zip(Json.encode(recordsBatch)));

    String expectedKafkaRecordKey = "1";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));
    when(kafkaInternalCache.containsByKey(eq(event.getId()))).thenReturn(true);

    // when
    Future<String> future = storedRecordChunksKafkaHandler.handle(kafkaRecord);

    // then
    verify(kafkaInternalCache, times(1)).containsByKey(eq(event.getId()));
    verify(recordsPublishingService, never()).sendEventsWithRecords(anyList(), anyString(), any(OkapiConnectionParams.class), anyString());
    assertTrue(future.succeeded());
    assertEquals(expectedKafkaRecordKey, future.result());
  }

}
