package org.folio.verticle.consumers;

import com.google.common.collect.Lists;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.commons.lang.StringUtils;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.RecordsMetadata;
import org.folio.services.ChunkProcessingService;
import org.folio.services.JobExecutionService;
import org.folio.services.RecordsPublishingService;
import org.folio.services.flowcontrol.RawRecordsFlowControlService;
import org.folio.verticle.consumers.util.JobExecutionUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.folio.rest.RestVerticle.OKAPI_HEADER_TENANT;
import static org.folio.rest.RestVerticle.OKAPI_HEADER_TOKEN;
import static org.folio.services.mappers.processor.MappingParametersProviderTest.SYSTEM_USER_ENABLED;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RawMarcChunksKafkaHandlerTest {
  private static final String TENANT_ID = "diku";
  private static final String TOKEN = "token";

  @Mock
  private RecordsPublishingService recordsPublishingService;
  @Mock
  private KafkaConsumerRecord<String, byte[]> kafkaRecord;
  @Mock
  private ChunkProcessingService eventDrivenChunkProcessingService;
  @Mock
  private RawRecordsFlowControlService flowControlService;
  @Mock
  private JobExecutionService jobExecutionService;

  private RawRecordsDto rawRecordsDto = new RawRecordsDto()
    .withInitialRecords(Lists.newArrayList(new InitialRecord()))
    .withRecordsMetadata(new RecordsMetadata().withContentType(RecordsMetadata.ContentType.MARC_JSON).withLast(true));

  private Vertx vertx = Vertx.vertx();
  private AsyncRecordHandler<String, byte[]> rawMarcChunksKafkaHandler;

  @Before
  public void setUp() {
    rawMarcChunksKafkaHandler = new RawMarcChunksKafkaHandler(eventDrivenChunkProcessingService, flowControlService, jobExecutionService, vertx);
  }

  @After
  public void invalidateCache() {
    JobExecutionUtils.clearCache();
  }

  @Test
  public void shouldNotHandleEventWhenJobExecutionWasCancelled() {
    when(kafkaRecord.headers()).thenReturn(List.of(KafkaHeader.header(OKAPI_HEADER_TENANT.toLowerCase(), TENANT_ID)));
    when(jobExecutionService.getJobExecutionById(any(), any())).thenReturn(Future.succeededFuture(Optional.of(new JobExecution().withStatus(JobExecution.Status.CANCELLED))));

    // when
    Future<String> future = rawMarcChunksKafkaHandler.handle(kafkaRecord);

    // then
    verify(recordsPublishingService, never()).sendEventsWithRecords(anyList(), anyString(), any(OkapiConnectionParams.class), anyString(), any());
    verify(flowControlService).triggerNextChunkFetch(TENANT_ID);
    assertTrue(future.succeeded());
  }

  @Test
  public void shouldHandleEvent() {
    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(Json.encode(rawRecordsDto));

    when(kafkaRecord.value()).thenReturn(Json.encode(event).getBytes(StandardCharsets.UTF_8));
    when(kafkaRecord.headers()).thenReturn(List.of(KafkaHeader.header(OKAPI_HEADER_TENANT.toLowerCase(), TENANT_ID),
      KafkaHeader.header(OKAPI_HEADER_TOKEN.toLowerCase(), TOKEN),
      KafkaHeader.header("jobExecutionId", UUID.randomUUID().toString())));
    when(jobExecutionService.getJobExecutionById(any(), any())).thenReturn(Future.succeededFuture(Optional.of(new JobExecution().withStatus(JobExecution.Status.PARSING_IN_PROGRESS))));
    when(eventDrivenChunkProcessingService.processChunk(any(), any(), any())).thenReturn(Future.succeededFuture(true));

    // when
    Future<String> future = rawMarcChunksKafkaHandler.handle(kafkaRecord);

    // then
    verify(recordsPublishingService, never()).sendEventsWithRecords(anyList(), anyString(), any(OkapiConnectionParams.class), anyString(), any());
    verify(eventDrivenChunkProcessingService).processChunk(any(), any(), argThat(params -> StringUtils.isNotEmpty(params.getToken())));
    assertTrue(future.succeeded());
  }

  @Test
  public void shouldHandleEventWhenSystemUserEnabled() {
    System.setProperty(SYSTEM_USER_ENABLED, "false");
    Event event = new Event()
      .withId(UUID.randomUUID().toString())
      .withEventPayload(Json.encode(rawRecordsDto));

    when(kafkaRecord.value()).thenReturn(Json.encode(event).getBytes(StandardCharsets.UTF_8));
    when(kafkaRecord.headers()).thenReturn(List.of(KafkaHeader.header(OKAPI_HEADER_TENANT.toLowerCase(), TENANT_ID),
      KafkaHeader.header(OKAPI_HEADER_TOKEN.toLowerCase(), TOKEN),
      KafkaHeader.header("jobExecutionId", UUID.randomUUID().toString())));
    when(jobExecutionService.getJobExecutionById(any(), any())).thenReturn(Future.succeededFuture(Optional.of(new JobExecution().withStatus(JobExecution.Status.PARSING_IN_PROGRESS))));
    when(eventDrivenChunkProcessingService.processChunk(any(), any(), any())).thenReturn(Future.succeededFuture(true));

    // when
    Future<String> future = rawMarcChunksKafkaHandler.handle(kafkaRecord);
    System.clearProperty(SYSTEM_USER_ENABLED);

    // then
    verify(recordsPublishingService, never()).sendEventsWithRecords(anyList(), anyString(), any(OkapiConnectionParams.class), anyString(), any());
    verify(eventDrivenChunkProcessingService).processChunk(any(), any(), argThat(params -> StringUtils.isEmpty(params.getToken())));
    assertTrue(future.succeeded());
  }


  @Test
  public void shouldNotHandleEventWhenIncorrectJobProfileIsPickedForUploadedFile() {
    var jobExecId = UUID.randomUUID().toString();
    when(kafkaRecord.headers()).thenReturn(List.of(KafkaHeader.header(OKAPI_HEADER_TENANT.toLowerCase(), TENANT_ID)));
    when(jobExecutionService.getJobExecutionById(any(), any())).thenReturn(Future.succeededFuture(Optional.of(new JobExecution().withId(jobExecId).withStatus(JobExecution.Status.PARSING_IN_PROGRESS))));
    // when error status is cached due to incorrect job profile is selected for uploaded file
    JobExecutionUtils.cache.put(jobExecId, JobExecution.Status.ERROR);
    // when
    Future<String> future = rawMarcChunksKafkaHandler.handle(kafkaRecord);

    // then
    verify(recordsPublishingService, never()).sendEventsWithRecords(anyList(), anyString(), any(OkapiConnectionParams.class), anyString(), any());
    assertTrue(future.succeeded());
  }
}
