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

//  @Mock
//  private EventProcessedService eventProcessedService;

  @InjectMocks
  private RawRecordsFlowControlServiceImpl service;

  @Before
  public void setUp() {
    ReflectionTestUtils.setField(service, "enableFlowControl", true);
  }

  @Test
  public void shouldSkipWhenFlowControlDisabled() {
    ReflectionTestUtils.setField(service, "enableFlowControl", false);
    ReflectionTestUtils.setField(service, "maxSimultaneousRecords", 10);
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
  public void shouldNotPauseWhenMaxSimultaneousRecordsNotExceeded() {
    ReflectionTestUtils.setField(service, "maxSimultaneousRecords", 20);
    service.trackChunkReceivedEvent(TENANT_ID, 15);
    service.increaseCounterInDb(TENANT_ID, 5);

    // 15 records not exceeds max 20, so no interaction
    verifyNoMoreInteractions(kafkaConsumersStorage);
  }

  @Test
  public void shouldNotPauseWhenMaxSimultaneousRecordsExceededAndConsumerAlreadyPaused() {
    ReflectionTestUtils.setField(service, "maxSimultaneousRecords", 20);

    KafkaConsumerWrapper<String, String> consumerWrapper = mock(KafkaConsumerWrapper.class);
    when(consumerWrapper.demand()).thenReturn(0L); // 0 means consumer already paused

    when(kafkaConsumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value()))
      .thenReturn(Collections.singletonList(consumerWrapper));
    service.increaseCounterInDb(TENANT_ID, 20);

    service.trackChunkReceivedEvent(TENANT_ID, 20);

    // 20 records exceeds max 20, but consumer already paused - no need to pause
    verify(kafkaConsumersStorage).getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());
    verify(consumerWrapper, never()).pause();
  }

  @Test
  public void shouldPauseWhenMaxSimultaneousRecordsExceededAndConsumerIsRunning() {
    ReflectionTestUtils.setField(service, "maxSimultaneousRecords", 20);

    KafkaConsumerWrapper<String, String> consumerWrapper = mock(KafkaConsumerWrapper.class);
    when(consumerWrapper.demand()).thenReturn(1L); // > 0 means that consumer running

    when(kafkaConsumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value()))
      .thenReturn(Collections.singletonList(consumerWrapper));

    service.increaseCounterInDb(TENANT_ID, 20);
    service.trackChunkReceivedEvent(TENANT_ID, 20);

    // 20 records exceeds max 20, consumer running - so need to pause
    verify(kafkaConsumersStorage).getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());
    verify(consumerWrapper).pause();
  }

  @Test
  public void shouldNotResumeWhenCompleteThresholdNotExceeded() {
    ReflectionTestUtils.setField(service, "maxSimultaneousRecords", 50);
    ReflectionTestUtils.setField(service, "recordsThreshold", 25);

    //service.increaseCounterInDb(TENANT_ID, 10);

    service.trackChunkReceivedEvent(TENANT_ID, 40);
//    service.trackChunkReceivedEvent(TENANT_ID, 10);
//    service.trackChunkReceivedEvent(TENANT_ID, 10);
    service.trackRecordCompleteEvent(TENANT_ID, 11);

    // 30 rec doesn't exceed max 50, after complete event 29 not less than 25 threshold, - so no any interaction with consumers storage
    verifyNoInteractions(kafkaConsumersStorage);
  }

  @Test
  public void shouldNotResumeWhenCompleteThresholdExceededAndConsumerAlreadyResumed() {
    ReflectionTestUtils.setField(service, "maxSimultaneousRecords", 10);
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
    ReflectionTestUtils.setField(service, "maxSimultaneousRecords", 10);
    ReflectionTestUtils.setField(service, "recordsThreshold", 5);

    KafkaConsumerWrapper<String, String> consumerWrapper = mock(KafkaConsumerWrapper.class);
    when(consumerWrapper.demand()).thenReturn(0L); // 0 means consumer already paused

    when(kafkaConsumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value()))
      .thenReturn(Collections.singletonList(consumerWrapper));

    service.increaseCounterInDb(TENANT_ID, 10);
    service.trackRecordCompleteEvent(TENANT_ID, 6);

    // after record complete event, current value 4 less than resume threshold, need to resume consumer
    verify(kafkaConsumersStorage).getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());
    verify(consumerWrapper).fetch(50);
  }

  @Test
  public void shouldReactToDuplicateEventAndResume() {
    ReflectionTestUtils.setField(service, "maxSimultaneousRecords", 5);
    ReflectionTestUtils.setField(service, "recordsThreshold", 3);

    KafkaConsumerWrapper<String, String> consumerBeforePause = mock(KafkaConsumerWrapper.class);
    when(consumerBeforePause.demand()).thenReturn(1L); // > 0 means consumer running

    when(kafkaConsumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value()))
      .thenReturn(Collections.singletonList(consumerBeforePause));

    service.trackChunkReceivedEvent(TENANT_ID, 5);

    KafkaConsumerWrapper<String, String> consumerBeforeResume = mock(KafkaConsumerWrapper.class);
    when(consumerBeforeResume.demand()).thenReturn(0L); // 0 means consumer is paused

    when(kafkaConsumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value()))
      .thenReturn(Collections.singletonList(consumerBeforeResume));

    service.trackChunkDuplicateEvent(TENANT_ID, 5);

    // 5 events comes and consumer paused, these events were duplicates and compensate 5 events came and after this consumer resumed
    verify(kafkaConsumersStorage, times(1)).getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());
    verify(consumerBeforePause).pause();
    //verify(consumerBeforeResume).resume();
  }
}
