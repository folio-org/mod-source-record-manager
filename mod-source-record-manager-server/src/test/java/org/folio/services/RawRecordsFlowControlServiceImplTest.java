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

    service.trackChunkReceivedEvent(TENANT_ID, 10);
    service.trackChunkDuplicateEvent(TENANT_ID, 10);
    service.trackRecordCompleteEvent(TENANT_ID, 0);

    // 1.firstly we receive 10 simultaneous records, so should pause consumers
    // 2.after it we recognize that these 10 records were duplicates, so should resume consumers
    // 3.after it receive 1 complete event that it less than 5 threshold, so should resume if not already resumed
    // But flow control feature is disabled, so we should no do all these actions
    verifyNoInteractions(kafkaConsumersStorage);
  }

  @Test
  public void shouldNotPauseWhenMaxSimultaneousChunksNotExceeded() {
    ReflectionTestUtils.setField(service, "maxSimultaneousChunks", 1);
    service.increaseCounterInDb(TENANT_ID, 25);
    service.trackChunkReceivedEvent(TENANT_ID, 50);

    verifyNoMoreInteractions(kafkaConsumersStorage);
  }

  @Test
  public void shouldFetchWhenMaxSimultaneousChunksLowThreshold() {
    ReflectionTestUtils.setField(service, "maxSimultaneousChunks", 1);
    ReflectionTestUtils.setField(service, "recordsThreshold", 25);

    KafkaConsumerWrapper<String, String> consumerWrapper = mock(KafkaConsumerWrapper.class);
    when(consumerWrapper.getId()).thenReturn(1);
    when(consumerWrapper.demand()).thenReturn(0L); // 0 means consumer already paused

    service.trackChunkReceivedEvent(TENANT_ID, 50);

    when(kafkaConsumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value()))
      .thenReturn(Collections.singletonList(consumerWrapper));

    service.trackRecordCompleteEvent(TENANT_ID, 26);

    // 24 low than threshold 25
    verify(kafkaConsumersStorage).getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());
    verify(consumerWrapper).fetch(1L);
  }

  @Test
  public void shouldEnableFetchWhenConsumerIsOnPause() {
    ReflectionTestUtils.setField(service, "maxSimultaneousChunks", 1);
    ReflectionTestUtils.setField(service, "recordsThreshold", 25);

    service.trackChunkReceivedEvent(TENANT_ID, 50);

    KafkaConsumerWrapper<String, String> consumerWrapper = mock(KafkaConsumerWrapper.class);
    when(consumerWrapper.demand()).thenReturn(1L); // > 0 means that consumer running

    when(kafkaConsumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value()))
      .thenReturn(Collections.singletonList(consumerWrapper));

    service.resetState();
    service.resetState();

    // 30 rec doesn't exceed max 50, after complete event 29 not less than 25 threshold, - so no any interaction with consumers storage
    verify(consumerWrapper).fetch(1L);
  }

  @Test
  public void shouldNotResumeWhenCompleteThresholdExceededAndConsumerAlreadyResumed() {
    ReflectionTestUtils.setField(service, "maxSimultaneousChunks", 10);
    ReflectionTestUtils.setField(service, "recordsThreshold", 5);

    KafkaConsumerWrapper<String, String> consumerWrapper = mock(KafkaConsumerWrapper.class);
    when(consumerWrapper.demand()).thenReturn(1L); // > 0 means that consumer running

    when(kafkaConsumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value()))
      .thenReturn(Collections.singletonList(consumerWrapper));

    service.increaseCounterInDb(TENANT_ID, 10);
    service.trackRecordCompleteEvent(TENANT_ID, 6);

    // after record complete event, current value 4 less than resume threshold, need to resume if paused (in our case consumer running)
    verify(kafkaConsumersStorage).getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());
    verify(consumerWrapper, never()).resume();
  }

  @Test
  public void shouldResumeWhenCompleteThresholdExceededAndConsumerPaused() {
    ReflectionTestUtils.setField(service, "maxSimultaneousChunks", 10);
    ReflectionTestUtils.setField(service, "recordsThreshold", 5);

    KafkaConsumerWrapper<String, String> consumerWrapper = mock(KafkaConsumerWrapper.class);
    when(consumerWrapper.demand()).thenReturn(0L); // 0 means consumer already paused

    when(kafkaConsumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value()))
      .thenReturn(Collections.singletonList(consumerWrapper));

    service.increaseCounterInDb(TENANT_ID, 10);
    service.trackRecordCompleteEvent(TENANT_ID, 6);

    // after record complete event, current value 4 less than resume threshold, need to resume consumer
    verify(kafkaConsumersStorage).getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());
    verify(consumerWrapper).fetch(10L);
  }

  @Test
  public void shouldReactToDuplicateEventAndFetchNextChunk() {
    ReflectionTestUtils.setField(service, "maxSimultaneousChunks", 1);
    ReflectionTestUtils.setField(service, "recordsThreshold", 5);

    KafkaConsumerWrapper<String, String> consumerBeforeResume = mock(KafkaConsumerWrapper.class);
    when(consumerBeforeResume.getId()).thenReturn(1);
    when(consumerBeforeResume.demand()).thenReturn(0L); // 0 means consumer is paused

    when(kafkaConsumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value()))
      .thenReturn(Collections.singletonList(consumerBeforeResume));

    service.trackChunkReceivedEvent(TENANT_ID, 50);
    service.trackChunkDuplicateEvent(TENANT_ID, 46);

    // 5 events comes and consumer paused, these events were duplicates and compensate 5 events came and after this consumer resumed
    verify(kafkaConsumersStorage, times(1)).getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());
    verify(consumerBeforeResume).fetch(1L);
  }
}
