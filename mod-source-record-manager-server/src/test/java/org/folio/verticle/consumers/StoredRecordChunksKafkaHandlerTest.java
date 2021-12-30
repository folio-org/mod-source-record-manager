package org.folio.verticle.consumers;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.impl.KafkaConsumerRecordImpl;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.folio.TestUtil;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.*;
import org.folio.services.EventProcessedService;
import org.folio.services.EventProcessedServiceImpl;
import org.folio.services.MappingRuleCache;
import org.folio.services.RecordsPublishingService;
import org.folio.services.journal.JournalService;
import org.folio.services.journal.JournalServiceImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.kafka.KafkaTopicNameHelper.getDefaultNameSpace;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;

@RunWith(VertxUnitRunner.class)
public class StoredRecordChunksKafkaHandlerTest extends AbstractRestTest {

  JournalService journalService;
  RecordsPublishingService recordsPublishingService;
  EventProcessedService eventProcessedService;
  MappingRuleCache mappingRuleCache;
  StoredRecordChunksKafkaHandler storedRecordChunksKafkaHandler;

  private String jobExecutionUUID = "5105b55a-b9a3-4f76-9402-a5243ea63c95";

  private JobExecution jobExecution = new JobExecution()
    .withId(jobExecutionUUID)
    .withHrId(1000)
    .withParentJobId(jobExecutionUUID)
    .withSubordinationType(JobExecution.SubordinationType.PARENT_SINGLE)
    .withStatus(JobExecution.Status.NEW)
    .withUiStatus(JobExecution.UiStatus.INITIALIZATION)
    .withSourcePath("importMarc.mrc")
    .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString()).withName("Marc jobs profile"))
    .withUserId(UUID.randomUUID().toString());

  private final JsonObject recordJson = new JsonObject()
    .put("id", UUID.randomUUID().toString())
    .put("snapshotId", jobExecutionUUID)
    .put("order", 1)
    .put("totalRecords", 1);

  private final HashMap<String, String> dataImportEventPayloadContext = new HashMap<>() {{
    put(INSTANCE.value(), recordJson.encode());
    put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
  }};


  @Before
  public void setUp() {
    eventProcessedService = getBeanFromSpringContext(vertx, EventProcessedServiceImpl.class);
    Assert.assertNotNull(eventProcessedService);

    journalService = getBeanFromSpringContext(vertx, JournalServiceImpl.class);
    Assert.assertNotNull(journalService);

    recordsPublishingService = getBeanFromSpringContext(vertx, RecordsPublishingService.class);
    Assert.assertNotNull(recordsPublishingService);

    mappingRuleCache = getBeanFromSpringContext(vertx, MappingRuleCache.class);
    Assert.assertNotNull(recordsPublishingService);

    storedRecordChunksKafkaHandler = new StoredRecordChunksKafkaHandler(recordsPublishingService, journalService, eventProcessedService, mappingRuleCache, vertx);
  }

  private static final String MARC_HOLDING_RECORD_PATH = "src/test/resources/org/folio/rest/marcHoldingRecord.json";

  @Test
  public void testProcessingDuplicateEvents(TestContext context) throws IOException {
    KafkaConsumerRecord<String, String> kafkaConsumerRecord = buildKafkaConsumerRecord();
    storedRecordChunksKafkaHandler.handle(kafkaConsumerRecord).onSuccess(
      res -> {
        storedRecordChunksKafkaHandler.handle(kafkaConsumerRecord)
          .onFailure(Assert::assertNotNull);
      }
    );
  }

  private KafkaConsumerRecord<String, String> buildKafkaConsumerRecord() throws IOException {
    String topic = KafkaTopicNameHelper.formatTopicName("folio", getDefaultNameSpace(), TENANT_ID, "DI_SRS_MARC_BIB_RECORD_CREATED");
    Record record = Json.decodeValue(TestUtil.readFileFromPath(MARC_HOLDING_RECORD_PATH), Record.class);
    RecordsBatchResponse savedRecordsBatch = new RecordsBatchResponse().withRecords(List.of(record)).withTotalRecords(1);
    Event event = new Event().withId(UUID.randomUUID().toString()).withEventPayload(Json.encode(savedRecordsBatch));
    ConsumerRecord<String, String> consumerRecord = buildConsumerRecord(topic, event);
    return new KafkaConsumerRecordImpl<>(consumerRecord);
  }

  private ConsumerRecord<String, String> buildConsumerRecord(String topic, Event event) {
    ConsumerRecord<java.lang.String, java.lang.String> consumerRecord = new ConsumerRecord("folio", 0, 0, topic, Json.encode(event));
    consumerRecord.headers().add(new RecordHeader(OKAPI_TENANT_HEADER, TENANT_ID.getBytes(StandardCharsets.UTF_8)));
    consumerRecord.headers().add(new RecordHeader(OKAPI_URL_HEADER, ("http://localhost:" + snapshotMockServer.port()).getBytes(StandardCharsets.UTF_8)));
    consumerRecord.headers().add(new RecordHeader(OKAPI_TOKEN_HEADER, (TOKEN).getBytes(StandardCharsets.UTF_8)));
    return consumerRecord;
  }

}
