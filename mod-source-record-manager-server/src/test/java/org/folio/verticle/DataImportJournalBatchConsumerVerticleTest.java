package org.folio.verticle;

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.processors.UnicastProcessor;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import io.vertx.rxjava3.kafka.client.consumer.KafkaConsumer;
import io.vertx.rxjava3.kafka.client.consumer.KafkaConsumerRecord;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.services.journal.BatchJournalService;
import org.folio.services.journal.BatchableJournalRecord;
import org.folio.util.JournalEvent;
import org.folio.verticle.consumers.util.EventTypeHandlerSelector;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Qualifier;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class DataImportJournalBatchConsumerVerticleTest {

  @Mock
  private BatchJournalService batchJournalService;

  // Add other mocked dependencies that are @Autowired in the verticle
  @Mock
  private EventTypeHandlerSelector eventTypeHandlerSelector;
  @Mock
  @Qualifier("newKafkaConfig")
  private KafkaConfig kafkaConfig;

  @InjectMocks
  private DataImportJournalBatchConsumerVerticle verticle;

  private io.vertx.rxjava3.core.Vertx rxVertx;

  @Before
  public void setUp() throws Exception {
    // Initialize mocks
    MockitoAnnotations.openMocks(this).close();

    Vertx vertx = Vertx.vertx();
    rxVertx = new io.vertx.rxjava3.core.Vertx(vertx);

    // Inject vertx and other dependencies manually if needed
    FieldUtils.writeField(verticle, "vertx", rxVertx, true);
    FieldUtils.writeField(verticle, "eventTypeHandlerSelector", eventTypeHandlerSelector, true);
    FieldUtils.writeField(verticle, "kafkaConfig", kafkaConfig, true);
  }

  @Test
  public void testSaveJournalRecordsSuccessfully() throws Exception {
    // Prepare test data
    String tenantId = "test-tenant";
    JournalRecord journalRecord = new JournalRecord()
      .withJobExecutionId("job123")
      .withSourceId("source123")
      .withEntityType(JournalRecord.EntityType.INSTANCE)
      .withActionType(JournalRecord.ActionType.CREATE)
      .withActionStatus(JournalRecord.ActionStatus.COMPLETED);

    BatchableJournalRecord batchableRecord = new BatchableJournalRecord(journalRecord, tenantId);
    List<BatchableJournalRecord> records = Collections.singletonList(batchableRecord);

    // Mock the service to succeed
    doAnswer(invocation -> {
      Handler<AsyncResult<Void>> handler = invocation.getArgument(2);
      handler.handle(Future.succeededFuture());
      return null;
    }).when(batchJournalService).saveBatchWithResponse(any(), eq(tenantId), any());

    // Create test data stream
    UnicastProcessor<Pair<Optional<DataImportJournalBatchConsumerVerticle.Bundle>, Collection<BatchableJournalRecord>>> processor =
      UnicastProcessor.create();

    // Add backpressure buffer to the flowable
    var flowable = processor
      .onBackpressureBuffer(1000)
      .serialize()
      .replay();

    // Create test subscriber that requests data in advance
    var testSubscriber = flowable.test(1);

    // Act
    Completable completable = verticle.saveJournalRecords(flowable);
    flowable.connect();

    // Send test data through the processor
    processor.onNext(Pair.of(Optional.empty(), records));
    processor.onComplete();

    // Verify completion with proper waiting
    TestObserver<Void> testObserver = completable.test();
    testSubscriber.awaitCount(1);  // Wait for the item to be processed
    testObserver.await(2, TimeUnit.SECONDS);  // Wait for completion

    // Verify service call
    ArgumentCaptor<List<JournalRecord>> recordsCaptor = ArgumentCaptor.forClass(List.class);
    verify(batchJournalService).saveBatchWithResponse(recordsCaptor.capture(), eq(tenantId), any());

    JournalRecord savedRecord = recordsCaptor.getValue().get(0);
    assertNotNull("JournalRecord ID should be set", savedRecord.getId());
    assertEquals("JobExecutionId should match", "job123", savedRecord.getJobExecutionId());
  }

  @Test
  public void testCommitKafkaEventsWhenSaveFails() throws Exception {
    // Prepare test data
    String tenantId = "test-tenant";
    JournalRecord journalRecord = new JournalRecord()
      .withJobExecutionId("job456")
      .withSourceId("source456")
      .withEntityType(JournalRecord.EntityType.HOLDINGS)
      .withActionType(JournalRecord.ActionType.UPDATE);

    BatchableJournalRecord batchableRecord = new BatchableJournalRecord(journalRecord, tenantId);
    List<BatchableJournalRecord> records = Collections.singletonList(batchableRecord);

    // Mock Kafka consumer components
    KafkaConsumer<String, byte[]> kafkaConsumerMock = mock(KafkaConsumer.class);
    io.vertx.kafka.client.consumer.KafkaConsumer<String, byte[]> vertxConsumerMock =
      mock(io.vertx.kafka.client.consumer.KafkaConsumer.class);
    when(kafkaConsumerMock.getDelegate()).thenReturn(vertxConsumerMock);
    FieldUtils.writeField(verticle, "kafkaConsumer", kafkaConsumerMock, true);

    // Mock service to fail
    doAnswer(invocation -> {
      Handler<AsyncResult<Void>> handler = invocation.getArgument(2);
      handler.handle(Future.failedFuture(new RuntimeException("Database error")));
      return null;
    }).when(batchJournalService).saveBatchWithResponse(any(), eq(tenantId), any());

    // Create test stream with Kafka bundle
    UnicastProcessor<Pair<Optional<DataImportJournalBatchConsumerVerticle.Bundle>, Collection<BatchableJournalRecord>>> processor =
      UnicastProcessor.create();

    var flowable = processor
      .onBackpressureBuffer(1000)
      .serialize()
      .replay();

    // Create Kafka bundle with test offset
    KafkaConsumerRecord<String, byte[]> recordMock = mock(KafkaConsumerRecord.class);
    when(recordMock.topic()).thenReturn("test-topic");
    when(recordMock.partition()).thenReturn(0);
    when(recordMock.offset()).thenReturn(123L);

    JournalEvent event = new JournalEvent();
    OkapiConnectionParams params = new OkapiConnectionParams(new HashMap<>(), Vertx.vertx());
    DataImportJournalBatchConsumerVerticle.Bundle bundle =
      new DataImportJournalBatchConsumerVerticle.Bundle(recordMock, event, params);

    // Act
    Completable completable = verticle.saveJournalRecords(flowable);
    flowable.connect();

    // Send data with Kafka bundle
    processor.onNext(Pair.of(Optional.of(bundle), records));
    processor.onComplete();

    // Verify
    TestObserver<Void> testObserver = completable.test();
    testObserver.await(2, TimeUnit.SECONDS);

    // 1. Verify service was called
    verify(batchJournalService).saveBatchWithResponse(any(), eq(tenantId), any());

    // 2. Verify commit was attempted with correct offset
    ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> offsetsCaptor =
      ArgumentCaptor.forClass(Map.class);
    verify(vertxConsumerMock, timeout(1000)).commit(offsetsCaptor.capture(), any());

    TopicPartition expectedTp = new TopicPartition("test-topic", 0);
    OffsetAndMetadata expectedOffset = new OffsetAndMetadata(124L, null);

    assertEquals("Should commit offset 124",
      expectedOffset.getOffset(),
      offsetsCaptor.getValue().get(expectedTp).getOffset());
  }
}
