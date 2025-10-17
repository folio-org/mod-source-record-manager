package org.folio.services;

import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.services.flowcontrol.RawRecordsFlowControlServiceImpl;
import org.folio.verticle.consumers.consumerstorage.KafkaConsumersStorage;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_READ;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class RawRecordsFlowControlServiceImplTest {

  private static final String TENANT_ID = "diku";

  @Mock
  private KafkaConsumersStorage kafkaConsumersStorage;

  @InjectMocks
  private RawRecordsFlowControlServiceImpl service;

  @Before
  public void setUp() {
    ReflectionTestUtils.setField(service, "enableFlowControl", true);
  }

  @Test
  public void shouldSkipWhenFlowControlDisabled() {
    ReflectionTestUtils.setField(service, "enableFlowControl", false);
    ReflectionTestUtils.setField(service, "maxSimultaneousChunks", 1);
    ReflectionTestUtils.setField(service, "recordsThreshold", 5);

    service.trackChunkReceivedEvent(TENANT_ID, 10, "");
    service.trackChunkDuplicateEvent(TENANT_ID, 10);
    service.trackRecordCompleteEvent(TENANT_ID, 0, "");

    verifyNoInteractions(kafkaConsumersStorage);
  }

  @Test
  public void shouldNotPauseWhenMaxSimultaneousChunksNotExceeded() {
    ReflectionTestUtils.setField(service, "maxSimultaneousChunks", 1);
    service.increaseCounterInDb(TENANT_ID, 25);
    service.trackChunkReceivedEvent(TENANT_ID, 50, "");

    verifyNoMoreInteractions(kafkaConsumersStorage);
  }

  @Test
  public void shouldFetchWhenMaxSimultaneousChunksLowThreshold() {
    ReflectionTestUtils.setField(service, "maxSimultaneousChunks", 1);
    ReflectionTestUtils.setField(service, "recordsThreshold", 25);

    KafkaConsumerWrapper<String, String> consumerWrapper = mock(KafkaConsumerWrapper.class);
    when(consumerWrapper.getId()).thenReturn(1);
    when(consumerWrapper.demand()).thenReturn(0L);

    service.trackChunkReceivedEvent(TENANT_ID, 50, "");

    when(kafkaConsumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value()))
      .thenReturn(Collections.singletonList(consumerWrapper));

    service.trackRecordCompleteEvent(TENANT_ID, 26, "");

    verify(kafkaConsumersStorage).getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());
    verify(consumerWrapper).fetch(1L);
  }

  @Test
  public void shouldEnableFetchWhenConsumerIsOnPause() {
    ReflectionTestUtils.setField(service, "maxSimultaneousChunks", 1);
    ReflectionTestUtils.setField(service, "recordsThreshold", 25);

    service.trackChunkReceivedEvent(TENANT_ID, 50, "");

    KafkaConsumerWrapper<String, String> consumerWrapper = mock(KafkaConsumerWrapper.class);
    when(consumerWrapper.demand()).thenReturn(1L);

    when(kafkaConsumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value()))
      .thenReturn(Collections.singletonList(consumerWrapper));

    service.resetState();
    service.resetState();

    verify(consumerWrapper).fetch(1L);
  }

  @Test
  public void shouldNotResumeWhenCompleteThresholdExceededAndConsumerAlreadyResumed() {
    ReflectionTestUtils.setField(service, "maxSimultaneousChunks", 10);
    ReflectionTestUtils.setField(service, "recordsThreshold", 5);

    KafkaConsumerWrapper<String, String> consumerWrapper = mock(KafkaConsumerWrapper.class);
    when(consumerWrapper.demand()).thenReturn(1L);

    when(kafkaConsumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value()))
      .thenReturn(Collections.singletonList(consumerWrapper));

    service.increaseCounterInDb(TENANT_ID, 10);
    service.trackRecordCompleteEvent(TENANT_ID, 6, "");

    verify(kafkaConsumersStorage).getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());
    verify(consumerWrapper, never()).resume();
  }

  @Test
  public void shouldResumeWhenCompleteThresholdExceededAndConsumerPaused() {
    ReflectionTestUtils.setField(service, "maxSimultaneousChunks", 10);
    ReflectionTestUtils.setField(service, "recordsThreshold", 5);

    KafkaConsumerWrapper<String, String> consumerWrapper = mock(KafkaConsumerWrapper.class);
    when(consumerWrapper.demand()).thenReturn(0L);

    when(kafkaConsumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value()))
      .thenReturn(Collections.singletonList(consumerWrapper));

    service.increaseCounterInDb(TENANT_ID, 10);
    service.trackRecordCompleteEvent(TENANT_ID, 6, "");

    verify(kafkaConsumersStorage).getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());
    verify(consumerWrapper).fetch(10L);
  }

  @Test
  public void shouldReactToDuplicateEventAndFetchNextChunk() {
    ReflectionTestUtils.setField(service, "maxSimultaneousChunks", 1);
    ReflectionTestUtils.setField(service, "recordsThreshold", 5);

    KafkaConsumerWrapper<String, String> consumerBeforeResume = mock(KafkaConsumerWrapper.class);
    when(consumerBeforeResume.getId()).thenReturn(1);
    when(consumerBeforeResume.demand()).thenReturn(0L);

    when(kafkaConsumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value()))
      .thenReturn(Collections.singletonList(consumerBeforeResume));

    service.trackChunkReceivedEvent(TENANT_ID, 50, "");
    service.trackChunkDuplicateEvent(TENANT_ID, 46);

    verify(kafkaConsumersStorage, times(1)).getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());
    verify(consumerBeforeResume).fetch(1L);
  }
}

