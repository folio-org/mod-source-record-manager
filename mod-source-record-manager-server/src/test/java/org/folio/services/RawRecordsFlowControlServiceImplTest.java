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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_RAW_RECORDS_CHUNK_READ;

@RunWith(MockitoJUnitRunner.class)
public class RawRecordsFlowControlServiceImplTest {

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
    ReflectionTestUtils.setField(service, "maxSimultaneousRecords", 10);

    service.trackChunkReceivedEvent(10);
    service.trackRecordCompleteEvent();

    verifyNoMoreInteractions(kafkaConsumersStorage);
  }

  @Test
  public void shouldNotPauseWhenMaxSimultaneousRecordsNotExceeded() {
    ReflectionTestUtils.setField(service, "maxSimultaneousRecords", 20);

    service.trackChunkReceivedEvent(5);
    service.trackChunkReceivedEvent(5);
    service.trackChunkReceivedEvent(5);

    verifyNoMoreInteractions(kafkaConsumersStorage);
  }

  @Test
  public void shouldNotPauseWhenMaxSimultaneousRecordsExceededAndConsumerAlreadyPaused() {
    ReflectionTestUtils.setField(service, "maxSimultaneousRecords", 20);

    KafkaConsumerWrapper<String, String> consumerWrapper = mock(KafkaConsumerWrapper.class);
    when(consumerWrapper.demand()).thenReturn(0L); // 0 means consumer already paused

    when(kafkaConsumersStorage.getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value()))
      .thenReturn(Collections.singletonList(consumerWrapper));

    service.trackChunkReceivedEvent(10);
    service.trackChunkReceivedEvent(10);

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

    service.trackChunkReceivedEvent(10);
    service.trackChunkReceivedEvent(10);

    verify(kafkaConsumersStorage).getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());
    verify(consumerWrapper).pause();
  }

  @Test
  public void shouldNotResumeWhenCompleteThresholdNotExceeded() {
    ReflectionTestUtils.setField(service, "maxSimultaneousRecords", 50);
    ReflectionTestUtils.setField(service, "recordsThreshold", 25);

    service.trackChunkReceivedEvent(10);
    service.trackChunkReceivedEvent(10);
    service.trackChunkReceivedEvent(10);
    service.trackRecordCompleteEvent();

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

    service.trackChunkReceivedEvent(5);
    service.trackRecordCompleteEvent();

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

    service.trackChunkReceivedEvent(5);
    service.trackRecordCompleteEvent();

    verify(kafkaConsumersStorage).getConsumersByEvent(DI_RAW_RECORDS_CHUNK_READ.value());
    verify(consumerWrapper).resume();
  }
}
