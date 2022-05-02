package org.folio.services;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.services.flowcontrol.FlowControlService;
import org.folio.verticle.consumers.consumerstorage.KafkaConsumersStorage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

@RunWith(MockitoJUnitRunner.class)
public class FlowControlServiceImplTest {

  @Mock
  private KafkaConsumersStorage kafkaConsumersStorage;

  @InjectMocks
  private FlowControlService service;

  @Before
  public void setUp() {
    ReflectionTestUtils.setField(service, "di.flow.max_simultaneous_records", 20);
    ReflectionTestUtils.setField(service, "di.flow.records_threshold", 10);
  }

  @Test
  public void shouldTrackChunkReceivedEvent() {
    service.trackChunkReceivedEvent(5);

  }
}
