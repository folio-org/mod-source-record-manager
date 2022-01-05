package org.folio.services;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.dao.EventProcessedDao;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.UUID;

import static org.mockito.Mockito.verify;

@RunWith(VertxUnitRunner.class)
public class EventProcessedServiceTest {

  @Mock
  private EventProcessedDao eventProcessedDao;

  @InjectMocks
  private EventProcessedService eventProcessedService = new EventProcessedServiceImpl(eventProcessedDao);

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void shouldCallDaoToGetProcessedEvent() {
    String eventId = UUID.randomUUID().toString();
    String handlerId = UUID.randomUUID().toString();
    String tenantId = "diku";

    eventProcessedService.collectData(eventId, handlerId, tenantId);

    verify(eventProcessedDao).save(eventId, handlerId, tenantId);
  }
}
