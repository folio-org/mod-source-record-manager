package org.folio.verticle.consumers;

import io.vertx.core.json.Json;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import net.mguenther.kafka.junit.KeyValue;
import net.mguenther.kafka.junit.ObserveKeyValues;
import net.mguenther.kafka.junit.SendKeyValues;
import org.folio.DataImportEventPayload;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.impl.AbstractRestTest;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_PARSED_RECORDS_CHUNK_SAVED;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(VertxUnitRunner.class)
public class StoredRecordChunkConsumersVerticleTest extends AbstractRestTest {

  private static final String JOB_EXECUTION_ID_HEADER = "jobExecutionId";
  private static final String ERROR_MSG_KEY = "ERROR";

  @Test
  public void shouldPublishDiErrorWhenLeaderRecordTypeValueIsInvalid() throws InterruptedException, IOException {
    // given
    // parsed content with invalid value '7' as record type identifier at the LEADER/06
    String parsedContentWithInvalidRecordTypeValue = "{\"leader\": \"13112c7m a2200553Ii 4500\"}";

    InitJobExecutionsRsDto response = constructAndPostInitJobExecutionRqDto(1);
    List<JobExecution> createdJobExecutions = response.getJobExecutions();
    assertThat(createdJobExecutions.size(), is(1));
    JobExecution jobExec = createdJobExecutions.get(0);

    RecordsBatchResponse recordsBatch = new RecordsBatchResponse()
      .withTotalRecords(1)
      .withRecords(List.of(new Record()
        .withRecordType(MARC_BIB)
        .withId(UUID.randomUUID().toString())
        .withSnapshotId(jobExec.getId())
        .withParsedRecord(new ParsedRecord().withContent(parsedContentWithInvalidRecordTypeValue))));

    Event event = new Event().withId(UUID.randomUUID().toString()).withEventPayload(ZIPArchiver.zip(Json.encode(recordsBatch)));
    KeyValue<String, String> kafkaRecord = new KeyValue<>("42", Json.encode(event));
    kafkaRecord.addHeader(OKAPI_TENANT_HEADER, TENANT_ID, UTF_8);
    kafkaRecord.addHeader(OKAPI_TOKEN_HEADER, TOKEN, UTF_8);
    kafkaRecord.addHeader(JOB_EXECUTION_ID_HEADER, jobExec.getId(), UTF_8);

    String topic = formatToKafkaTopicName(DI_PARSED_RECORDS_CHUNK_SAVED.value());
    SendKeyValues<String, String> request = SendKeyValues.to(topic, Collections.singletonList(kafkaRecord))
      .useDefaults();

    // when
    kafkaCluster.send(request);

    // then
    String topicToObserve = formatToKafkaTopicName(DI_ERROR.value());
    List<String> observedValues = kafkaCluster.observeValues(ObserveKeyValues.on(topicToObserve, 1)
      .observeFor(30, TimeUnit.SECONDS)
      .build());

    Event obtainedEvent = Json.decodeValue(observedValues.get(0), Event.class);
    DataImportEventPayload eventPayload = Json.decodeValue(ZIPArchiver.unzip(obtainedEvent.getEventPayload()), DataImportEventPayload.class);
    assertEquals(DI_ERROR.value(), eventPayload.getEventType());
    assertEquals(jobExec.getId(), eventPayload.getJobExecutionId());
    assertEquals(TENANT_ID, eventPayload.getTenant());
    assertNotNull(eventPayload.getContext().get(EntityType.MARC_BIBLIOGRAPHIC.value()));
    assertNotNull(eventPayload.getContext().get(ERROR_MSG_KEY));
  }
}
