package org.folio.verticle.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.folio.TestUtil;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.services.ChunkProcessingService;
import org.folio.services.EventProcessedService;
import org.folio.services.JobExecutionService;
import org.folio.services.MappingRuleCache;
import org.folio.services.RecordsPublishingService;
import org.folio.services.flowcontrol.RawRecordsFlowControlService;
import org.folio.services.journal.JournalService;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.folio.rest.RestVerticle.OKAPI_HEADER_TENANT;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RawMarcChunksKafkaHandlerTest {
  private static final String TENANT_ID = "diku";
  private static final String MAPPING_RULES_PATH = "src/test/resources/org/folio/services/marc_bib_rules.json";

  private static JsonObject mappingRules;

  @Mock
  private RecordsPublishingService recordsPublishingService;
  @Mock
  private KafkaConsumerRecord<String, byte[]> kafkaRecord;
  @Mock
  private JournalService journalService;
  @Mock
  private EventProcessedService eventProcessedService;
  @Mock
  private ChunkProcessingService eventDrivenChunkProcessingService;
  @Mock
  private RawRecordsFlowControlService flowControlService;
  @Mock
  private MappingRuleCache mappingRuleCache;
  @Mock
  private JobExecutionService jobExecutionService;
  @Captor
  private ArgumentCaptor<JsonArray> journalRecordsCaptor;

  private Vertx vertx = Vertx.vertx();
  private AsyncRecordHandler<String, byte[]> rawMarcChunksKafkaHandler;
  @BeforeClass
  public static void setUpClass() throws IOException {
    mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
  }

  @Before
  public void setUp() {
    rawMarcChunksKafkaHandler = new RawMarcChunksKafkaHandler(eventDrivenChunkProcessingService, flowControlService, jobExecutionService, vertx);
//    when(jobExecutionService.getJobExecutionById(anyString(), anyString()))
//      .thenReturn(Future.succeededFuture(Optional.of(new JobExecution())));
  }

  @Test
  public void shouldNotHandleEventWhenJobExecutionWasCancelled() {
    when(kafkaRecord.headers()).thenReturn(List.of(KafkaHeader.header(OKAPI_HEADER_TENANT.toLowerCase(), TENANT_ID)));
    when(jobExecutionService.getJobExecutionById(any(), any())).thenReturn(Future.succeededFuture(Optional.of(new JobExecution().withStatus(JobExecution.Status.CANCELLED))));

    // when
    Future<String> future = rawMarcChunksKafkaHandler.handle(kafkaRecord);

    // then
    verify(recordsPublishingService, never()).sendEventsWithRecords(anyList(), anyString(), any(OkapiConnectionParams.class), anyString());
    assertTrue(future.succeeded());
  }
}
