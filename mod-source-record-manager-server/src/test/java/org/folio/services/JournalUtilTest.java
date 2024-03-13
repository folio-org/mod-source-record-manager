package org.folio.services;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.DataImportEventPayload;
import org.folio.Record;
import org.folio.rest.jaxrs.model.ErrorRecord;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.IncomingRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.RawRecord;
import org.folio.services.journal.JournalRecordMapperException;
import org.folio.services.journal.JournalUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.DataImportEventTypes.DI_ERROR;
import static org.folio.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_UPDATED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.COMPLETED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.ERROR;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.CREATE;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.NON_MATCH;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.UPDATE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.AUTHORITY;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_HOLDINGS;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.PO_LINE;
import static org.folio.services.journal.JournalUtil.ERROR_KEY;
import static org.folio.services.journal.JournalUtil.MARC_BIB_RECORD_CREATED;

@RunWith(VertxUnitRunner.class)
public class JournalUtilTest {

  private static final String CENTRAL_TENANT_ID_KEY = "CENTRAL_TENANT_ID";
  private static final String CURRENT_EVENT_TYPE = "CURRENT_EVENT_TYPE";
  public static final String INCOMING_RECORD_ID = "INCOMING_RECORD_ID";

  @Test
  public void shouldBuildJournalRecordsByRecordsWithoutError() {
    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();

    org.folio.rest.jaxrs.model.Record record = new org.folio.rest.jaxrs.model.Record()
      .withId(recordId)
      .withSnapshotId(snapshotId)
      .withOrder(0)
      .withRecordType(org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByRecords(List.of(record));

    assertThat(journalRecords).hasSize(1);
    assertThat(journalRecords.get(0).getId()).isNotBlank();
    assertThat(journalRecords.get(0).getJobExecutionId()).isEqualTo(snapshotId);
    assertThat(journalRecords.get(0).getSourceId()).isEqualTo(recordId);
    assertThat(journalRecords.get(0).getSourceRecordOrder()).isEqualTo(record.getOrder());
    assertThat(journalRecords.get(0).getActionType()).isEqualTo(JournalRecord.ActionType.PARSE);
    assertThat(journalRecords.get(0).getActionDate()).isNotNull();
    assertThat(journalRecords.get(0).getActionStatus()).isEqualTo(JournalRecord.ActionStatus.COMPLETED);
    assertThat(journalRecords.get(0).getEntityType()).isNull();
    assertThat(journalRecords.get(0).getError()).isNull();
  }

  @Test
  public void shouldBuildJournalRecordsByRecordsWithError() {
    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();

    ErrorRecord errorRecord = new ErrorRecord().withDescription("error");
    org.folio.rest.jaxrs.model.Record record = new org.folio.rest.jaxrs.model.Record()
      .withId(recordId)
      .withSnapshotId(snapshotId)
      .withOrder(0)
      .withRecordType(org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB)
      .withErrorRecord(errorRecord);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByRecords(List.of(record));

    assertThat(journalRecords).hasSize(1);
    assertThat(journalRecords.get(0).getId()).isNotBlank();
    assertThat(journalRecords.get(0).getJobExecutionId()).isEqualTo(snapshotId);
    assertThat(journalRecords.get(0).getSourceId()).isEqualTo(recordId);
    assertThat(journalRecords.get(0).getSourceRecordOrder()).isEqualTo(record.getOrder());
    assertThat(journalRecords.get(0).getActionType()).isEqualTo(JournalRecord.ActionType.PARSE);
    assertThat(journalRecords.get(0).getActionDate()).isNotNull();
    assertThat(journalRecords.get(0).getActionStatus()).isEqualTo(ERROR);
    assertThat(journalRecords.get(0).getEntityType()).isNull();
    assertThat(journalRecords.get(0).getError()).isEqualTo(errorRecord.getDescription());
  }

  @Test
  public void shouldBuildIncomingRecordsByRecords() {
    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();

    org.folio.rest.jaxrs.model.Record record = new org.folio.rest.jaxrs.model.Record()
      .withId(recordId)
      .withSnapshotId(snapshotId)
      .withOrder(0)
      .withRawRecord(new RawRecord().withContent("rawRecord"))
      .withRecordType(org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB)
      .withParsedRecord(new ParsedRecord().withContent("parsedRecord"));

    List<IncomingRecord> incomingRecords = JournalUtil.buildIncomingRecordsByRecords(List.of(record));

    assertThat(incomingRecords).hasSize(1);
    assertThat(incomingRecords.get(0).getId()).isEqualTo(record.getId());
    assertThat(incomingRecords.get(0).getJobExecutionId()).isEqualTo(snapshotId);
    assertThat(incomingRecords.get(0).getOrder()).isEqualTo(record.getOrder());
    assertThat(incomingRecords.get(0).getRawRecordContent()).isEqualTo("rawRecord");
    assertThat(incomingRecords.get(0).getRecordType()).isEqualTo(IncomingRecord.RecordType.MARC_BIB);
    assertThat(incomingRecords.get(0).getParsedRecordContent()).isEqualTo("parsedRecord");
  }

  @Test
  public void shouldBuildJournalRecordForInstance() throws JournalRecordMapperException {
    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = UUID.randomUUID().toString();

    JsonObject instanceJson = new JsonObject()
      .put("id", instanceId)
      .put("hrid", instanceHrid);

    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();
    String incomingRecordId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    HashMap<String, String> context = new HashMap<>();
    context.put(INSTANCE.value(), instanceJson.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    context.put(INCOMING_RECORD_ID, incomingRecordId);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_INSTANCE_CREATED")
      .withContext(context);

    List<JournalRecord> journalRecord = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, INSTANCE, COMPLETED);

    Assert.assertNotNull(journalRecord);
    Assert.assertEquals(snapshotId, journalRecord.get(0).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecord.get(0).getSourceId());
    Assert.assertEquals(1, journalRecord.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(INSTANCE, journalRecord.get(0).getEntityType());
    Assert.assertEquals(instanceId, journalRecord.get(0).getEntityId());
    Assert.assertEquals(instanceHrid, journalRecord.get(0).getEntityHrId());
    Assert.assertEquals(CREATE, journalRecord.get(0).getActionType());
    Assert.assertEquals(COMPLETED, journalRecord.get(0).getActionStatus());
    Assert.assertNotNull(journalRecord.get(0).getActionDate());
  }

  @Test
  public void shouldBuildTwoJournalRecordWithInstanceCreatedEvent() throws JournalRecordMapperException {
    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = UUID.randomUUID().toString();

    JsonObject instanceJson = new JsonObject()
      .put("id", instanceId)
      .put("hrid", instanceHrid);

    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();
    String incomingRecordId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    HashMap<String, String> context = new HashMap<>();
    context.put(INSTANCE.value(), instanceJson.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    context.put(CURRENT_EVENT_TYPE, "DI_INVENTORY_INSTANCE_CREATED");
    context.put(INCOMING_RECORD_ID, incomingRecordId);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_COMPLETED")
      .withContext(context);

    List<JournalRecord> journalRecord = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, INSTANCE, COMPLETED);

    Assert.assertNotNull(journalRecord);
    Assert.assertEquals(2, journalRecord.size());
    Assert.assertEquals(snapshotId, journalRecord.get(0).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecord.get(0).getSourceId());
    Assert.assertEquals(1, journalRecord.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(INSTANCE, journalRecord.get(0).getEntityType());
    Assert.assertEquals(instanceId, journalRecord.get(0).getEntityId());
    Assert.assertEquals(instanceHrid, journalRecord.get(0).getEntityHrId());
    Assert.assertEquals(CREATE, journalRecord.get(0).getActionType());
    Assert.assertEquals(COMPLETED, journalRecord.get(0).getActionStatus());
    Assert.assertNotNull(journalRecord.get(0).getActionDate());
  }

  @Test
  public void shouldBuildJournalRecordForAuthority() throws JournalRecordMapperException {
    String authorityId = UUID.randomUUID().toString();

    JsonObject authorityJson = new JsonObject()
      .put("id", authorityId);

    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();
    String incomingRecordId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    HashMap<String, String> context = new HashMap<>();
    context.put(AUTHORITY.value(), authorityJson.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    context.put(INCOMING_RECORD_ID, incomingRecordId);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_COMPLETED")
      .withContext(context);

    List<JournalRecord> journalRecord = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, AUTHORITY, COMPLETED);

    Assert.assertNotNull(journalRecord);
    Assert.assertEquals(snapshotId, journalRecord.get(0).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecord.get(0).getSourceId());
    Assert.assertEquals(1, journalRecord.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(AUTHORITY, journalRecord.get(0).getEntityType());
    Assert.assertEquals(authorityId, journalRecord.get(0).getEntityId());
    Assert.assertNull(journalRecord.get(0).getEntityHrId());
    Assert.assertEquals(CREATE, journalRecord.get(0).getActionType());
    Assert.assertEquals(COMPLETED, journalRecord.get(0).getActionStatus());
    Assert.assertNotNull(journalRecord.get(0).getActionDate());
  }

  @Test(expected = JournalRecordMapperException.class)
  public void shouldThrowExceptionInstanceIsInvalid() throws JournalRecordMapperException {
    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    HashMap<String, String> context = new HashMap<>();
    context.put(INSTANCE.value(), "test");
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_INSTANCE_CREATED")
      .withContext(context);

    JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, INSTANCE, COMPLETED);
  }

  @Test
  public void shouldBuildJournalRecordWhenNoEntityPresent() throws JournalRecordMapperException {
    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();
    String incomingRecordId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    context.put("NOT_MATCHED_NUMBER", "1");
    context.put(INCOMING_RECORD_ID, incomingRecordId);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_HOLDING_NOT_MATCHED")
      .withContext(context);

    List<JournalRecord> journalRecord = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      NON_MATCH, HOLDINGS, COMPLETED);

    Assert.assertNotNull(journalRecord);
    Assert.assertEquals(snapshotId, journalRecord.get(0).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecord.get(0).getSourceId());
    Assert.assertEquals(1, journalRecord.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(HOLDINGS, journalRecord.get(0).getEntityType());
    Assert.assertEquals(NON_MATCH, journalRecord.get(0).getActionType());
    Assert.assertEquals(COMPLETED, journalRecord.get(0).getActionStatus());
    Assert.assertNotNull(journalRecord.get(0).getActionDate());
  }

  @Test
  public void shouldBuildJournalRecordForSingleHolding() throws JournalRecordMapperException {
    String instanceId = UUID.randomUUID().toString();
    String holdingsId = UUID.randomUUID().toString();
    String holdingsHrid = UUID.randomUUID().toString();

    JsonObject holdingsJson = new JsonObject()
      .put("id", holdingsId)
      .put("hrid", holdingsHrid)
      .put("instanceId", instanceId);

    String recordId = UUID.randomUUID().toString();
    String incomingRecordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), holdingsJson.encode());
    context.put(MARC_HOLDINGS.value(), recordJson.encode());
    context.put(INCOMING_RECORD_ID, incomingRecordId);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_HOLDING_CREATED")
      .withContext(context);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, HOLDINGS, COMPLETED);

    assertForHoldings(instanceId, holdingsId, holdingsHrid, recordId, snapshotId, journalRecords, incomingRecordId);
  }

  @Test
  public void shouldBuildJournalRecordForHolding() throws JournalRecordMapperException {
    String instanceId = UUID.randomUUID().toString();
    String holdingsId = UUID.randomUUID().toString();
    String holdingsHrid = UUID.randomUUID().toString();

    JsonObject holdingsJson = new JsonObject()
      .put("id", holdingsId)
      .put("hrid", holdingsHrid)
      .put("instanceId", instanceId);

    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();
    String incomingRecordId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    JsonArray multipleHoldings = new JsonArray();
    multipleHoldings.add(holdingsJson);

    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), String.valueOf(multipleHoldings.encode()));
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    context.put(INCOMING_RECORD_ID, incomingRecordId);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_HOLDING_CREATED")
      .withContext(context);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, HOLDINGS, COMPLETED);

    assertForHoldings(instanceId, holdingsId, holdingsHrid, recordId, snapshotId, journalRecords, incomingRecordId);
  }

  private static void assertForHoldings(String instanceId, String holdingsId, String holdingsHrid, String recordId,
                                        String snapshotId, List<JournalRecord> journalRecords, String incomingRecordId) {
    Assert.assertNotNull(journalRecords);
    Assert.assertEquals(snapshotId, journalRecords.get(0).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecords.get(0).getSourceId());
    Assert.assertEquals(1, journalRecords.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(HOLDINGS, journalRecords.get(0).getEntityType());
    Assert.assertEquals(holdingsId, journalRecords.get(0).getEntityId());
    Assert.assertEquals(holdingsHrid, journalRecords.get(0).getEntityHrId());
    Assert.assertEquals(instanceId, journalRecords.get(0).getInstanceId());
    Assert.assertEquals(CREATE, journalRecords.get(0).getActionType());
    Assert.assertEquals(COMPLETED, journalRecords.get(0).getActionStatus());
    Assert.assertNotNull(journalRecords.get(0).getActionDate());
  }

  @Test
  public void shouldBuildJournalRecordForItemWhenInstanceIsPopulated() throws JournalRecordMapperException {
    String itemId = UUID.randomUUID().toString();
    String itemHrid = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = UUID.randomUUID().toString();
    String holdingsId = UUID.randomUUID().toString();

    JsonObject itemJson = new JsonObject()
      .put("id", itemId)
      .put("hrid", itemHrid)
      .put("holdingsRecordId", holdingsId);

    JsonObject instanceJson = new JsonObject()
      .put("id", instanceId)
      .put("hrid", instanceHrid);

    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();
    String incomingRecordId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    JsonArray multipleItems = new JsonArray();
    multipleItems.add(itemJson);

    HashMap<String, String> context = new HashMap<>();
    context.put(ITEM.value(), String.valueOf(multipleItems));
    context.put(INSTANCE.value(), instanceJson.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    context.put(INCOMING_RECORD_ID, incomingRecordId);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_ITEM_CREATED")
      .withContext(context);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, ITEM, COMPLETED);

    Assert.assertNotNull(journalRecords);
    Assert.assertEquals(snapshotId, journalRecords.get(0).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecords.get(0).getSourceId());
    Assert.assertEquals(1, journalRecords.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(ITEM, journalRecords.get(0).getEntityType());
    Assert.assertEquals(itemId, journalRecords.get(0).getEntityId());
    Assert.assertEquals(itemHrid, journalRecords.get(0).getEntityHrId());
    Assert.assertEquals(instanceId, journalRecords.get(0).getInstanceId());
    Assert.assertEquals(holdingsId, journalRecords.get(0).getHoldingsId());
    Assert.assertEquals(CREATE, journalRecords.get(0).getActionType());
    Assert.assertEquals(COMPLETED, journalRecords.get(0).getActionStatus());
    Assert.assertNotNull(journalRecords.get(0).getActionDate());
  }

  @Test
  public void shouldBuildJournalRecordForItemWhenInstanceIsNotPopulated() throws JournalRecordMapperException {
    String itemId = UUID.randomUUID().toString();
    String itemHrid = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();
    String holdingsId = UUID.randomUUID().toString();
    String holdingsHrid = UUID.randomUUID().toString();

    JsonObject itemJson = new JsonObject()
      .put("id", itemId)
      .put("hrid", itemHrid)
      .put("holdingsRecordId", holdingsId);

    JsonObject holdingsJson = new JsonObject()
      .put("id", holdingsId)
      .put("hrid", holdingsHrid)
      .put("instanceId", instanceId);

    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();
    String incomingRecordId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    JsonArray multipleItems = new JsonArray();
    multipleItems.add(itemJson);

    JsonArray multipleHoldings = new JsonArray();
    multipleHoldings.add(holdingsJson);

    HashMap<String, String> context = new HashMap<>();
    context.put(ITEM.value(), String.valueOf(multipleItems));
    context.put(HOLDINGS.value(), String.valueOf(multipleHoldings));
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    context.put(INCOMING_RECORD_ID, incomingRecordId);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_ITEM_CREATED")
      .withContext(context);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, ITEM, COMPLETED);

    Assert.assertNotNull(journalRecords);
    Assert.assertEquals(snapshotId, journalRecords.get(0).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecords.get(0).getSourceId());
    Assert.assertEquals(1, journalRecords.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(ITEM, journalRecords.get(0).getEntityType());
    Assert.assertEquals(itemId, journalRecords.get(0).getEntityId());
    Assert.assertEquals(itemHrid, journalRecords.get(0).getEntityHrId());
    Assert.assertEquals(instanceId, journalRecords.get(0).getInstanceId());
    Assert.assertEquals(holdingsId, journalRecords.get(0).getHoldingsId());
    Assert.assertEquals(CREATE, journalRecords.get(0).getActionType());
    Assert.assertEquals(COMPLETED, journalRecords.get(0).getActionStatus());
    Assert.assertNotNull(journalRecords.get(0).getActionDate());
  }

  @Test
  public void shouldBuildJournalRecordWhenNoRecord() throws JournalRecordMapperException {
    String testError = "Something Happened";
    String testJobExecutionId = UUID.randomUUID().toString();
    HashMap<String, String> context = new HashMap<>();
    context.put(ERROR_KEY, testError);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withJobExecutionId(testJobExecutionId)
      .withContext(context);

    List<JournalRecord> journalRecord = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, JournalRecord.EntityType.EDIFACT, COMPLETED);

    Assert.assertNotNull(journalRecord);
    Assert.assertEquals(0, journalRecord.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(testError, journalRecord.get(0).getError());
    Assert.assertEquals(testJobExecutionId, journalRecord.get(0).getJobExecutionId());
    Assert.assertEquals(JournalRecord.EntityType.EDIFACT, journalRecord.get(0).getEntityType());
    Assert.assertEquals(CREATE, journalRecord.get(0).getActionType());
    Assert.assertEquals(COMPLETED, journalRecord.get(0).getActionStatus());
    Assert.assertNotNull(journalRecord.get(0).getActionDate());
  }

  @Test
  public void shouldBuildJournalRecordForInstanceEvenIfEntityIsNotExists() throws JournalRecordMapperException {
    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();
    String incomingRecordId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    context.put(INCOMING_RECORD_ID, incomingRecordId);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_INSTANCE_CREATED")
      .withContext(context);

    List<JournalRecord> journalRecord = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, INSTANCE, COMPLETED);

    Assert.assertNotNull(journalRecord);
    Assert.assertEquals(snapshotId, journalRecord.get(0).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecord.get(0).getSourceId());
    Assert.assertEquals(1, journalRecord.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(INSTANCE, journalRecord.get(0).getEntityType());
    Assert.assertEquals(CREATE, journalRecord.get(0).getActionType());
    Assert.assertEquals(COMPLETED, journalRecord.get(0).getActionStatus());
    Assert.assertNotNull(journalRecord.get(0).getActionDate());
  }

  @Test
  public void shouldBuildJournalRecordForOrderCreated() throws JournalRecordMapperException {
    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();
    String incomingRecordId = UUID.randomUUID().toString();
    String entityId = UUID.randomUUID().toString();
    String purchaseOrderId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    JsonObject orderJson = new JsonObject()
      .put("id", entityId)
      .put("purchaseOrderId", purchaseOrderId);

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    context.put(PO_LINE.value(), orderJson.encode());
    context.put(INCOMING_RECORD_ID, incomingRecordId);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_COMPLETED")
      .withEventsChain(Collections.singletonList("DI_ORDER_CREATED"))
      .withContext(context);

    List<JournalRecord> journalRecord = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, PO_LINE, COMPLETED);

    Assert.assertNotNull(journalRecord);
    Assert.assertEquals(snapshotId, journalRecord.get(0).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecord.get(0).getSourceId());
    Assert.assertEquals(entityId, journalRecord.get(0).getEntityId());
    Assert.assertEquals(purchaseOrderId, journalRecord.get(0).getOrderId());
    Assert.assertEquals(1, journalRecord.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(PO_LINE, journalRecord.get(0).getEntityType());
    Assert.assertEquals(CREATE, journalRecord.get(0).getActionType());
    Assert.assertEquals(COMPLETED, journalRecord.get(0).getActionStatus());
    Assert.assertNotNull(journalRecord.get(0).getActionDate());
  }

  @Test
  public void shouldBuildSeveralJournalRecordsForMultipleHolding() throws JournalRecordMapperException {
    String instanceId = UUID.randomUUID().toString();
    String firstHoldingsId = UUID.randomUUID().toString();
    String secondHoldingsId = UUID.randomUUID().toString();
    String firstHoldingsHrid = UUID.randomUUID().toString();
    String secondHoldingsHrid = UUID.randomUUID().toString();
    String firstPermanentLocationId = UUID.randomUUID().toString();
    String secondPermanentLocationId = UUID.randomUUID().toString();

    JsonObject firstHoldingsAsJson = new JsonObject()
      .put("id", firstHoldingsId)
      .put("hrid", firstHoldingsHrid)
      .put("instanceId", instanceId)
      .put("permanentLocationId", firstPermanentLocationId);

    JsonObject secondHoldingsAsJson = new JsonObject()
      .put("id", secondHoldingsId)
      .put("hrid", secondHoldingsHrid)
      .put("instanceId", instanceId)
      .put("permanentLocationId", secondPermanentLocationId);

    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();
    String incomingRecordId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    JsonArray multipleHoldings = new JsonArray();
    multipleHoldings.add(firstHoldingsAsJson);
    multipleHoldings.add(secondHoldingsAsJson);

    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), String.valueOf(multipleHoldings.encode()));
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    context.put(INCOMING_RECORD_ID, incomingRecordId);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_HOLDING_CREATED")
      .withContext(context);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, HOLDINGS, COMPLETED);

    Assert.assertNotNull(journalRecords);
    Assert.assertEquals(2, journalRecords.size());

    Assert.assertEquals(snapshotId, journalRecords.get(0).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecords.get(0).getSourceId());
    Assert.assertEquals(1, journalRecords.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(HOLDINGS, journalRecords.get(0).getEntityType());
    Assert.assertEquals(firstHoldingsId, journalRecords.get(0).getEntityId());
    Assert.assertEquals(firstHoldingsHrid, journalRecords.get(0).getEntityHrId());
    Assert.assertEquals(instanceId, journalRecords.get(0).getInstanceId());
    Assert.assertEquals(CREATE, journalRecords.get(0).getActionType());
    Assert.assertEquals(COMPLETED, journalRecords.get(0).getActionStatus());
    Assert.assertNotNull(journalRecords.get(0).getActionDate());
    Assert.assertEquals(firstPermanentLocationId, journalRecords.get(0).getPermanentLocationId());

    Assert.assertEquals(snapshotId, journalRecords.get(1).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecords.get(1).getSourceId());
    Assert.assertEquals(1, journalRecords.get(1).getSourceRecordOrder().intValue());
    Assert.assertEquals(HOLDINGS, journalRecords.get(1).getEntityType());
    Assert.assertEquals(secondHoldingsId, journalRecords.get(1).getEntityId());
    Assert.assertEquals(secondHoldingsHrid, journalRecords.get(1).getEntityHrId());
    Assert.assertEquals(instanceId, journalRecords.get(1).getInstanceId());
    Assert.assertEquals(CREATE, journalRecords.get(1).getActionType());
    Assert.assertEquals(COMPLETED, journalRecords.get(1).getActionStatus());
    Assert.assertNotNull(journalRecords.get(1).getActionDate());
    Assert.assertEquals(secondPermanentLocationId, journalRecords.get(1).getPermanentLocationId());
  }

  @Test
  public void shouldBuildSeveralJournalRecordForMultipleItemsWhenInstanceIsPopulated() throws JournalRecordMapperException {
    String firstItemId = UUID.randomUUID().toString();
    String firstItemHrid = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = UUID.randomUUID().toString();
    String firstHoldingsId = UUID.randomUUID().toString();

    String secondItemId = UUID.randomUUID().toString();
    String secondItemHrid = UUID.randomUUID().toString();
    String secondHoldingsId = UUID.randomUUID().toString();

    JsonObject firstItemJson = new JsonObject()
      .put("id", firstItemId)
      .put("hrid", firstItemHrid)
      .put("holdingsRecordId", firstHoldingsId);

    JsonObject secondItemJson = new JsonObject()
      .put("id", secondItemId)
      .put("hrid", secondItemHrid)
      .put("holdingId", secondHoldingsId);

    JsonObject instanceJson = new JsonObject()
      .put("id", instanceId)
      .put("hrid", instanceHrid);

    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();
    String incomingRecordId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    JsonArray multipleItems = new JsonArray();
    multipleItems.add(firstItemJson);
    multipleItems.add(secondItemJson);

    HashMap<String, String> context = new HashMap<>();
    context.put(ITEM.value(), String.valueOf(multipleItems));
    context.put(INSTANCE.value(), instanceJson.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    context.put(INCOMING_RECORD_ID, incomingRecordId);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_ITEM_CREATED")
      .withContext(context);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, ITEM, COMPLETED);

    Assert.assertNotNull(journalRecords);
    Assert.assertEquals(2, journalRecords.size());

    Assert.assertEquals(snapshotId, journalRecords.get(0).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecords.get(0).getSourceId());
    Assert.assertEquals(1, journalRecords.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(ITEM, journalRecords.get(0).getEntityType());
    Assert.assertEquals(firstItemId, journalRecords.get(0).getEntityId());
    Assert.assertEquals(firstItemHrid, journalRecords.get(0).getEntityHrId());
    Assert.assertEquals(instanceId, journalRecords.get(0).getInstanceId());
    Assert.assertEquals(firstHoldingsId, journalRecords.get(0).getHoldingsId());
    Assert.assertEquals(CREATE, journalRecords.get(0).getActionType());
    Assert.assertEquals(COMPLETED, journalRecords.get(0).getActionStatus());
    Assert.assertNotNull(journalRecords.get(0).getActionDate());

    Assert.assertEquals(snapshotId, journalRecords.get(1).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecords.get(1).getSourceId());
    Assert.assertEquals(1, journalRecords.get(1).getSourceRecordOrder().intValue());
    Assert.assertEquals(ITEM, journalRecords.get(1).getEntityType());
    Assert.assertEquals(secondItemId, journalRecords.get(1).getEntityId());
    Assert.assertEquals(secondItemHrid, journalRecords.get(1).getEntityHrId());
    Assert.assertEquals(instanceId, journalRecords.get(1).getInstanceId());
    Assert.assertEquals(secondHoldingsId, journalRecords.get(1).getHoldingsId());
    Assert.assertEquals(CREATE, journalRecords.get(1).getActionType());
    Assert.assertEquals(COMPLETED, journalRecords.get(1).getActionStatus());
    Assert.assertNotNull(journalRecords.get(1).getActionDate());
  }

  @Test
  public void shouldBuildSeveralJournalRecordsForMultipleItemsWhenInstanceIsNotPopulatedAndItemsAreInTheSameHolding() throws JournalRecordMapperException {
    String firstItemId = UUID.randomUUID().toString();
    String firstItemHrid = UUID.randomUUID().toString();
    String secondItemId = UUID.randomUUID().toString();
    String secondItemHrid = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();
    String holdingsId = UUID.randomUUID().toString();
    String holdingsHrid = UUID.randomUUID().toString();

    JsonObject firstItemJson = new JsonObject()
      .put("id", firstItemId)
      .put("hrid", firstItemHrid)
      .put("holdingsRecordId", holdingsId);

    JsonObject secondItemJson = new JsonObject()
      .put("id", secondItemId)
      .put("hrid", secondItemHrid)
      .put("holdingId", holdingsId);

    JsonObject holdingsJson = new JsonObject()
      .put("id", holdingsId)
      .put("hrid", holdingsHrid)
      .put("instanceId", instanceId);

    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();
    String incomingRecordId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    JsonArray multipleItems = new JsonArray();
    multipleItems.add(firstItemJson);
    multipleItems.add(secondItemJson);

    JsonArray multipleHoldings = new JsonArray();
    multipleHoldings.add(holdingsJson);

    HashMap<String, String> context = new HashMap<>();
    context.put(ITEM.value(), String.valueOf(multipleItems));
    context.put(HOLDINGS.value(), String.valueOf(multipleHoldings));
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    context.put(INCOMING_RECORD_ID, incomingRecordId);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_ITEM_CREATED")
      .withContext(context);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, ITEM, COMPLETED);

    Assert.assertNotNull(journalRecords);
    Assert.assertEquals(2, journalRecords.size());

    Assert.assertEquals(snapshotId, journalRecords.get(0).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecords.get(0).getSourceId());
    Assert.assertEquals(1, journalRecords.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(ITEM, journalRecords.get(0).getEntityType());
    Assert.assertEquals(firstItemId, journalRecords.get(0).getEntityId());
    Assert.assertEquals(firstItemHrid, journalRecords.get(0).getEntityHrId());
    Assert.assertEquals(instanceId, journalRecords.get(0).getInstanceId());
    Assert.assertEquals(holdingsId, journalRecords.get(0).getHoldingsId());
    Assert.assertEquals(CREATE, journalRecords.get(0).getActionType());
    Assert.assertEquals(COMPLETED, journalRecords.get(0).getActionStatus());
    Assert.assertNotNull(journalRecords.get(0).getActionDate());

    Assert.assertEquals(snapshotId, journalRecords.get(1).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecords.get(1).getSourceId());
    Assert.assertEquals(1, journalRecords.get(1).getSourceRecordOrder().intValue());
    Assert.assertEquals(ITEM, journalRecords.get(1).getEntityType());
    Assert.assertEquals(secondItemId, journalRecords.get(1).getEntityId());
    Assert.assertEquals(secondItemHrid, journalRecords.get(1).getEntityHrId());
    Assert.assertEquals(instanceId, journalRecords.get(1).getInstanceId());
    Assert.assertEquals(holdingsId, journalRecords.get(1).getHoldingsId());
    Assert.assertEquals(CREATE, journalRecords.get(1).getActionType());
    Assert.assertEquals(COMPLETED, journalRecords.get(1).getActionStatus());
    Assert.assertNotNull(journalRecords.get(1).getActionDate());
  }

  @Test
  public void shouldBuildSeveralJournalRecordsWithErrorsIfMultipleErrorsExists() throws JournalRecordMapperException {
    String itemId = UUID.randomUUID().toString();
    String itemHrid = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = UUID.randomUUID().toString();
    String holdingsId = UUID.randomUUID().toString();
    String firstErrorUUID = UUID.randomUUID().toString();
    String secondErrorUUID = UUID.randomUUID().toString();

    JsonObject itemJson = new JsonObject()
      .put("id", itemId)
      .put("hrid", itemHrid)
      .put("holdingsRecordId", holdingsId);

    JsonObject instanceJson = new JsonObject()
      .put("id", instanceId)
      .put("hrid", instanceHrid);

    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();
    String incomingRecordId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    JsonArray multipleItems = new JsonArray();
    multipleItems.add(itemJson);

    JsonObject firstError = new JsonObject();
    firstError.put("id", firstErrorUUID);
    firstError.put("error", "Testing first error message!");
    firstError.put("holdingId", holdingsId);

    JsonObject secondError = new JsonObject();
    secondError.put("id", secondErrorUUID);
    secondError.put("error", "Testing second error message!");
    secondError.put("holdingId", holdingsId);

    JsonArray multipleErrors = new JsonArray();
    multipleErrors.add(firstError);
    multipleErrors.add(secondError);

    HashMap<String, String> context = new HashMap<>();
    context.put(ITEM.value(), String.valueOf(multipleItems));
    context.put(INSTANCE.value(), instanceJson.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    context.put("ERRORS", String.valueOf(multipleErrors));
    context.put(INCOMING_RECORD_ID, incomingRecordId);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_ITEM_CREATED")
      .withContext(context);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, ITEM, COMPLETED);

    Assert.assertNotNull(journalRecords);
    Assert.assertEquals(3, journalRecords.size());

    Assert.assertEquals(snapshotId, journalRecords.get(0).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecords.get(0).getSourceId());
    Assert.assertEquals(1, journalRecords.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(ITEM, journalRecords.get(0).getEntityType());
    Assert.assertEquals(itemId, journalRecords.get(0).getEntityId());
    Assert.assertEquals(itemHrid, journalRecords.get(0).getEntityHrId());
    Assert.assertEquals(instanceId, journalRecords.get(0).getInstanceId());
    Assert.assertEquals(holdingsId, journalRecords.get(0).getHoldingsId());
    Assert.assertEquals(CREATE, journalRecords.get(0).getActionType());
    Assert.assertEquals(COMPLETED, journalRecords.get(0).getActionStatus());
    Assert.assertNotNull(journalRecords.get(0).getActionDate());

    Assert.assertEquals(snapshotId, journalRecords.get(1).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecords.get(1).getSourceId());
    Assert.assertEquals(1, journalRecords.get(1).getSourceRecordOrder().intValue());
    Assert.assertEquals(ITEM, journalRecords.get(1).getEntityType());
    Assert.assertEquals(firstErrorUUID, journalRecords.get(1).getEntityId());
    Assert.assertEquals(CREATE, journalRecords.get(1).getActionType());
    Assert.assertEquals(ERROR, journalRecords.get(1).getActionStatus());
    Assert.assertEquals("Testing first error message!", journalRecords.get(1).getError());
    Assert.assertEquals(holdingsId, journalRecords.get(1).getHoldingsId());

    Assert.assertEquals(snapshotId, journalRecords.get(2).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecords.get(2).getSourceId());
    Assert.assertEquals(1, journalRecords.get(2).getSourceRecordOrder().intValue());
    Assert.assertEquals(ITEM, journalRecords.get(2).getEntityType());
    Assert.assertEquals(secondErrorUUID, journalRecords.get(2).getEntityId());
    Assert.assertEquals(CREATE, journalRecords.get(2).getActionType());
    Assert.assertEquals(ERROR, journalRecords.get(2).getActionStatus());
    Assert.assertEquals(holdingsId, journalRecords.get(2).getHoldingsId());

    Assert.assertNotNull(journalRecords.get(1).getActionDate());
  }

  @Test
  public void shouldBuildSingleErrorJournalRecordIfDiErrorDuringMultipleImport() throws JournalRecordMapperException {
    String itemId = UUID.randomUUID().toString();
    String itemHrid = UUID.randomUUID().toString();
    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = UUID.randomUUID().toString();
    String holdingsId = UUID.randomUUID().toString();
    String firstErrorUUID = UUID.randomUUID().toString();
    String secondErrorUUID = UUID.randomUUID().toString();

    JsonObject itemJson = new JsonObject()
      .put("id", itemId)
      .put("hrid", itemHrid)
      .put("holdingId", holdingsId);

    JsonObject instanceJson = new JsonObject()
      .put("id", instanceId)
      .put("hrid", instanceHrid);

    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();
    String incomingRecordId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    JsonArray multipleItems = new JsonArray();
    multipleItems.add(itemJson);

    JsonObject firstError = new JsonObject();
    firstError.put("id", firstErrorUUID);
    firstError.put("error", "Testing first error message!");

    JsonObject secondError = new JsonObject();
    secondError.put("id", secondErrorUUID);
    secondError.put("error", "Testing second error message!");

    JsonArray multipleErrors = new JsonArray();
    multipleErrors.add(firstError);
    multipleErrors.add(secondError);

    HashMap<String, String> context = new HashMap<>();
    context.put(ITEM.value(), String.valueOf(multipleItems));
    context.put(INSTANCE.value(), instanceJson.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    context.put("ERROR", String.valueOf(multipleErrors));
    context.put(INCOMING_RECORD_ID, incomingRecordId);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_ERROR")
      .withContext(context);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, ITEM, ERROR);

    Assert.assertNotNull(journalRecords);
    Assert.assertEquals(1, journalRecords.size());

    Assert.assertEquals(snapshotId, journalRecords.get(0).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecords.get(0).getSourceId());
    Assert.assertEquals(1, journalRecords.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(ITEM, journalRecords.get(0).getEntityType());
    Assert.assertEquals(CREATE, journalRecords.get(0).getActionType());
    Assert.assertEquals(ERROR, journalRecords.get(0).getActionStatus());
    Assert.assertEquals(String.valueOf(multipleErrors), journalRecords.get(0).getError());

    Assert.assertNotNull(journalRecords.get(0).getActionDate());
  }

  @Test
  public void shouldBuildJournalRecordWithCentralTenantIdFromPayload() throws JournalRecordMapperException {
    String expectedCentralTenantId = "mobius";
    String incomingRecordId = UUID.randomUUID().toString();

    Record record = new Record()
      .withId(UUID.randomUUID().toString())
      .withSnapshotId(UUID.randomUUID().toString())
      .withOrder(1);

    HashMap<String, String> context = new HashMap<>() {{
      put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
      put(CENTRAL_TENANT_ID_KEY, expectedCentralTenantId);
      put(INCOMING_RECORD_ID, incomingRecordId);
    }};

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_UPDATED.value())
      .withContext(context);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      UPDATE, MARC_BIBLIOGRAPHIC, COMPLETED);

    Assert.assertEquals(1, journalRecords.size());
    Assert.assertEquals(incomingRecordId, journalRecords.get(0).getSourceId());
    Assert.assertEquals(1, journalRecords.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(expectedCentralTenantId, journalRecords.get(0).getTenantId());
    Assert.assertEquals(MARC_BIBLIOGRAPHIC, journalRecords.get(0).getEntityType());
    Assert.assertEquals(UPDATE, journalRecords.get(0).getActionType());
    Assert.assertEquals(COMPLETED, journalRecords.get(0).getActionStatus());
  }

  @Test
  public void shouldBuildJournalRecordForMarcBibliographicUpdate() throws JournalRecordMapperException {
    String incomingRecordId = UUID.randomUUID().toString();

    Record record = new Record()
      .withId(UUID.randomUUID().toString())
      .withSnapshotId(UUID.randomUUID().toString())
      .withOrder(1);

    HashMap<String, String> context = new HashMap<>() {{
      put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
      put(INCOMING_RECORD_ID, incomingRecordId);
    }};

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_BIB_RECORD_UPDATED.value())
      .withContext(context);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      UPDATE, MARC_BIBLIOGRAPHIC, COMPLETED);

    Assert.assertEquals(1, journalRecords.size());
    Assert.assertEquals(incomingRecordId, journalRecords.get(0).getSourceId());
    Assert.assertEquals(1, journalRecords.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(record.getId(), journalRecords.get(0).getEntityId());
    Assert.assertEquals(MARC_BIBLIOGRAPHIC, journalRecords.get(0).getEntityType());
    Assert.assertEquals(UPDATE, journalRecords.get(0).getActionType());
    Assert.assertEquals(COMPLETED, journalRecords.get(0).getActionStatus());
    Assert.assertEquals(MARC_BIBLIOGRAPHIC, journalRecords.get(0).getEntityType());
  }


  @Test
  public void shouldBuildJournalRecordForNonMatchWithErrorAndMatchedNumberNotAvailable() throws JournalRecordMapperException {
    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();
    String incomingRecordId = UUID.randomUUID().toString();
    String expectedErrorMessage = "matching error message";

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    context.put(ERROR_KEY, expectedErrorMessage);
    context.put(INCOMING_RECORD_ID, incomingRecordId);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_ERROR")
      .withContext(context);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      NON_MATCH, ITEM, ERROR);

    Assert.assertNotNull(journalRecords);
    Assert.assertEquals(1, journalRecords.size());

    JournalRecord journalRecord = journalRecords.get(0);
    Assert.assertEquals(snapshotId, journalRecord.getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecord.getSourceId());
    Assert.assertEquals(1, journalRecord.getSourceRecordOrder().intValue());
    Assert.assertEquals(ITEM, journalRecord.getEntityType());
    Assert.assertEquals(NON_MATCH, journalRecord.getActionType());
    Assert.assertEquals(ERROR, journalRecord.getActionStatus());
    Assert.assertEquals(expectedErrorMessage, journalRecord.getError());
    Assert.assertNotNull(journalRecord.getActionDate());
  }

  @Test
  public void shouldReturnUpdatedInstanceAndCreatedMarcBibJournalRecordInMarcBibStatusTrue() throws JournalRecordMapperException {
    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = UUID.randomUUID().toString();

    JsonObject instanceJson = new JsonObject()
      .put("id", instanceId)
      .put("hrid", instanceHrid);

    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();
    String incomingRecordId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    HashMap<String, String> context = new HashMap<>();
    context.put(INSTANCE.value(), instanceJson.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    context.put(MARC_BIB_RECORD_CREATED, Boolean.TRUE.toString());
    context.put(CURRENT_EVENT_TYPE, "DI_INVENTORY_INSTANCE_UPDATED");
    context.put(INCOMING_RECORD_ID, incomingRecordId);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_COMPLETED")
      .withContext(context);

    List<JournalRecord> journalRecord = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      UPDATE, INSTANCE, COMPLETED);

    Assert.assertNotNull(journalRecord);
    Assert.assertEquals(2, journalRecord.size());
    Assert.assertEquals(snapshotId, journalRecord.get(0).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecord.get(0).getSourceId());
    Assert.assertEquals(1, journalRecord.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(INSTANCE, journalRecord.get(0).getEntityType());
    Assert.assertEquals(instanceId, journalRecord.get(0).getEntityId());
    Assert.assertEquals(instanceHrid, journalRecord.get(0).getEntityHrId());
    Assert.assertEquals(UPDATE, journalRecord.get(0).getActionType());
    Assert.assertEquals(COMPLETED, journalRecord.get(0).getActionStatus());
    Assert.assertNotNull(journalRecord.get(0).getActionDate());
    Assert.assertEquals(CREATE, journalRecord.get(1).getActionType());
    Assert.assertEquals(COMPLETED, journalRecord.get(1).getActionStatus());
    Assert.assertEquals(MARC_BIBLIOGRAPHIC, journalRecord.get(1).getEntityType());
  }

  @Test
  public void shouldReturnUpdatedInstanceAndCreatedMarcBibJournalRecordInMarcBibStatusFalse() throws JournalRecordMapperException {
    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = UUID.randomUUID().toString();

    JsonObject instanceJson = new JsonObject()
      .put("id", instanceId)
      .put("hrid", instanceHrid);

    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();
    String incomingRecordId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    HashMap<String, String> context = new HashMap<>();
    context.put(INSTANCE.value(), instanceJson.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    context.put(MARC_BIB_RECORD_CREATED, Boolean.FALSE.toString());
    context.put(CURRENT_EVENT_TYPE, "DI_INVENTORY_INSTANCE_UPDATED");
    context.put(INCOMING_RECORD_ID, incomingRecordId);

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_COMPLETED")
      .withContext(context);

    List<JournalRecord> journalRecord = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      UPDATE, INSTANCE, COMPLETED);

    Assert.assertNotNull(journalRecord);
    Assert.assertEquals(2, journalRecord.size());

    Assert.assertEquals(snapshotId, journalRecord.get(0).getJobExecutionId());
    Assert.assertEquals(incomingRecordId, journalRecord.get(0).getSourceId());
    Assert.assertEquals(1, journalRecord.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(INSTANCE, journalRecord.get(0).getEntityType());
    Assert.assertEquals(instanceId, journalRecord.get(0).getEntityId());
    Assert.assertEquals(instanceHrid, journalRecord.get(0).getEntityHrId());
    Assert.assertEquals(UPDATE, journalRecord.get(0).getActionType());
    Assert.assertEquals(COMPLETED, journalRecord.get(0).getActionStatus());
    Assert.assertNotNull(journalRecord.get(0).getActionDate());

    Assert.assertEquals(UPDATE, journalRecord.get(1).getActionType());
    Assert.assertEquals(COMPLETED, journalRecord.get(1).getActionStatus());
    Assert.assertEquals(MARC_BIBLIOGRAPHIC, journalRecord.get(1).getEntityType());
  }

}
