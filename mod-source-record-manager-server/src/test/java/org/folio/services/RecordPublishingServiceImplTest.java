package org.folio.services;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.util.ReflectionTestUtils;

import org.folio.TestUtil;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaConfig;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.util.EventHandlingUtil;

@RunWith(MockitoJUnitRunner.class)
public class RecordPublishingServiceImplTest {

  private static final String MARC_BIB_RECORD_PATH = "src/test/resources/org/folio/rest/record.json";

  private final OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(new HashMap<>(), Vertx.vertx());

  @Mock
  private JobExecutionService jobExecutionService;
  @Mock
  private KafkaConfig kafkaConfig;
  @Mock
  private DataImportPayloadContextBuilder payloadContextBuilder;
  @InjectMocks
  private RecordsPublishingServiceImpl recordsPublishingService =
    new RecordsPublishingServiceImpl(jobExecutionService, payloadContextBuilder, kafkaConfig);

  @Before
  public void setUp() {
    ReflectionTestUtils.setField(recordsPublishingService, "maxDistributionNum", 10);
  }

  @Test
  @SneakyThrows
  public void shouldPopulateEventPayloadWithCorrelationIdWhenHeaderExists() {
    Record record = Json.decodeValue(TestUtil.readFileFromPath(MARC_BIB_RECORD_PATH), Record.class);
    String jobExecutionId = UUID.randomUUID().toString();

    when(jobExecutionService.getJobExecutionById(anyString(), anyString()))
      .thenReturn(Future.succeededFuture(Optional.of(getTestJobExecution())));

    try (var mockedStatic = Mockito.mockStatic(EventHandlingUtil.class)) {
      mockedStatic.when(() -> EventHandlingUtil.sendEventToKafka(any(), any(), any(), any(), any(), any()))
        .thenReturn(Future.succeededFuture(true));

      Boolean resultSucceeded = recordsPublishingService.sendEventsWithRecords(Collections.singletonList(record),
        jobExecutionId,
        okapiConnectionParams,
        DI_SRS_MARC_BIB_RECORD_CREATED.value()).result();

      assertTrue(resultSucceeded);
      mockedStatic.verify(() -> EventHandlingUtil.populatePayloadWithHeadersData(any(DataImportEventPayload.class), any()));
    }
  }

  private JobExecution getTestJobExecution() {
    return new JobExecution().withId(UUID.randomUUID().toString())
      .withJobProfileSnapshotWrapper(new ProfileSnapshotWrapper()
        .withChildSnapshotWrappers(Collections.singletonList(new ProfileSnapshotWrapper())))
      .withJobProfileInfo(new JobProfileInfo()
        .withId(UUID.randomUUID().toString())
        .withName("test").withDataType(JobProfileInfo.DataType.MARC));
  }
}
