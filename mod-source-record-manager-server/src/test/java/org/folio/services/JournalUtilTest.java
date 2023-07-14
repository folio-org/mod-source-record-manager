package org.folio.services;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.services.journal.JournalRecordMapperException;
import org.folio.services.journal.JournalUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static org.folio.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionStatus.COMPLETED;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.CREATE;
import static org.folio.rest.jaxrs.model.JournalRecord.ActionType.NON_MATCH;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.PO_LINE;
import static org.folio.services.journal.JournalUtil.ERROR_KEY;

@RunWith(VertxUnitRunner.class)
public class JournalUtilTest {

  @Test
  public void shouldBuildJournalRecordForInstance() throws JournalRecordMapperException {
    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = UUID.randomUUID().toString();

    JsonObject instanceJson = new JsonObject()
      .put("id", instanceId)
      .put("hrid", instanceHrid);

    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    HashMap<String, String> context = new HashMap<>();
    context.put(INSTANCE.value(), instanceJson.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_INSTANCE_CREATED")
      .withContext(context);

    List<JournalRecord>  journalRecord = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, INSTANCE, COMPLETED);

    Assert.assertNotNull(journalRecord);
    Assert.assertEquals(snapshotId, journalRecord.get(0).getJobExecutionId());
    Assert.assertEquals(recordId, journalRecord.get(0).getSourceId());
    Assert.assertEquals(1, journalRecord.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(INSTANCE, journalRecord.get(0).getEntityType());
    Assert.assertEquals(instanceId, journalRecord.get(0).getEntityId());
    Assert.assertEquals(instanceHrid, journalRecord.get(0).getEntityHrId());
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

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_HOLDING_NOT_MATCHED")
      .withContext(context);

    List<JournalRecord> journalRecord = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      NON_MATCH, HOLDINGS, COMPLETED);

    Assert.assertNotNull(journalRecord);
    Assert.assertEquals(snapshotId, journalRecord.get(0).getJobExecutionId());
    Assert.assertEquals(recordId, journalRecord.get(0).getSourceId());
    Assert.assertEquals(1, journalRecord.get(0).getSourceRecordOrder().intValue());
    Assert.assertEquals(HOLDINGS, journalRecord.get(0).getEntityType());
    Assert.assertEquals(NON_MATCH, journalRecord.get(0).getActionType());
    Assert.assertEquals(COMPLETED, journalRecord.get(0).getActionStatus());
    Assert.assertNotNull(journalRecord.get(0).getActionDate());
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

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    JsonArray multipleHoldings = new JsonArray();
    multipleHoldings.add(holdingsJson.encode());

    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), String.valueOf(multipleHoldings.encode()));
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_HOLDING_CREATED")
      .withContext(context);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, HOLDINGS, COMPLETED);

    Assert.assertNotNull(journalRecords);
    Assert.assertEquals(snapshotId, journalRecords.get(0).getJobExecutionId());
    Assert.assertEquals(recordId, journalRecords.get(0).getSourceId());
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

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    JsonArray multipleItems = new JsonArray();
    multipleItems.add(itemJson.encode());

    HashMap<String, String> context = new HashMap<>();
    context.put(ITEM.value(), String.valueOf(multipleItems));
    context.put(INSTANCE.value(), instanceJson.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_ITEM_CREATED")
      .withContext(context);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, ITEM, COMPLETED);

    Assert.assertNotNull(journalRecords);
    Assert.assertEquals(snapshotId, journalRecords.get(0).getJobExecutionId());
    Assert.assertEquals(recordId, journalRecords.get(0).getSourceId());
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

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    JsonArray multipleItems = new JsonArray();
    multipleItems.add(itemJson.encode());

    JsonArray multipleHoldings = new JsonArray();
    multipleHoldings.add(holdingsJson.encode());

    HashMap<String, String> context = new HashMap<>();
    context.put(ITEM.value(), String.valueOf(multipleItems));
    context.put(HOLDINGS.value(), String.valueOf(multipleHoldings));
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_ITEM_CREATED")
      .withContext(context);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, ITEM, COMPLETED);

    Assert.assertNotNull(journalRecords);
    Assert.assertEquals(snapshotId, journalRecords.get(0).getJobExecutionId());
    Assert.assertEquals(recordId, journalRecords.get(0).getSourceId());
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

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_INSTANCE_CREATED")
      .withContext(context);

    List<JournalRecord> journalRecord = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, INSTANCE, COMPLETED);

    Assert.assertNotNull(journalRecord);
    Assert.assertEquals(snapshotId, journalRecord.get(0).getJobExecutionId());
    Assert.assertEquals(recordId, journalRecord.get(0).getSourceId());
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

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_COMPLETED")
      .withEventsChain(Collections.singletonList("DI_ORDER_CREATED"))
      .withContext(context);

    List<JournalRecord> journalRecord = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, PO_LINE, COMPLETED);

    Assert.assertNotNull(journalRecord);
    Assert.assertEquals(snapshotId, journalRecord.get(0).getJobExecutionId());
    Assert.assertEquals(recordId, journalRecord.get(0).getSourceId());
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

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    JsonArray multipleHoldings = new JsonArray();
    multipleHoldings.add(firstHoldingsAsJson.encode());
    multipleHoldings.add(secondHoldingsAsJson.encode());


    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), String.valueOf(multipleHoldings.encode()));
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_HOLDING_CREATED")
      .withContext(context);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, HOLDINGS, COMPLETED);

    Assert.assertNotNull(journalRecords);
    Assert.assertEquals(2, journalRecords.size());

    Assert.assertEquals(snapshotId, journalRecords.get(0).getJobExecutionId());
    Assert.assertEquals(recordId, journalRecords.get(0).getSourceId());
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
    Assert.assertEquals(recordId, journalRecords.get(1).getSourceId());
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
      .put("holdingsRecordId", secondHoldingsId);

    JsonObject instanceJson = new JsonObject()
      .put("id", instanceId)
      .put("hrid", instanceHrid);

    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    JsonArray multipleItems = new JsonArray();
    multipleItems.add(firstItemJson.encode());
    multipleItems.add(secondItemJson.encode());


    HashMap<String, String> context = new HashMap<>();
    context.put(ITEM.value(), String.valueOf(multipleItems));
    context.put(INSTANCE.value(), instanceJson.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_ITEM_CREATED")
      .withContext(context);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, ITEM, COMPLETED);

    Assert.assertNotNull(journalRecords);
    Assert.assertEquals(2, journalRecords.size());

    Assert.assertEquals(snapshotId, journalRecords.get(0).getJobExecutionId());
    Assert.assertEquals(recordId, journalRecords.get(0).getSourceId());
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
    Assert.assertEquals(recordId, journalRecords.get(1).getSourceId());
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
      .put("holdingsRecordId", holdingsId);

    JsonObject holdingsJson = new JsonObject()
      .put("id", holdingsId)
      .put("hrid", holdingsHrid)
      .put("instanceId", instanceId);

    String recordId = UUID.randomUUID().toString();
    String snapshotId = UUID.randomUUID().toString();

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    JsonArray multipleItems = new JsonArray();
    multipleItems.add(firstItemJson.encode());
    multipleItems.add(secondItemJson.encode());

    JsonArray multipleHoldings = new JsonArray();
    multipleHoldings.add(holdingsJson.encode());

    HashMap<String, String> context = new HashMap<>();
    context.put(ITEM.value(), String.valueOf(multipleItems));
    context.put(HOLDINGS.value(), String.valueOf(multipleHoldings));
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_ITEM_CREATED")
      .withContext(context);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, ITEM, COMPLETED);

    Assert.assertNotNull(journalRecords);
    Assert.assertEquals(2, journalRecords.size());

    Assert.assertEquals(snapshotId, journalRecords.get(0).getJobExecutionId());
    Assert.assertEquals(recordId, journalRecords.get(0).getSourceId());
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
    Assert.assertEquals(recordId, journalRecords.get(1).getSourceId());
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

    JsonObject recordJson = new JsonObject()
      .put("id", recordId)
      .put("snapshotId", snapshotId)
      .put("order", 1);

    JsonArray multipleItems = new JsonArray();
    multipleItems.add(itemJson.encode());

    JsonObject firstError = new JsonObject();
    firstError.put("id", firstErrorUUID);
    firstError.put("error", "Testing first error message!");

    JsonObject secondError = new JsonObject();
    secondError.put("id", secondErrorUUID);
    secondError.put("error", "Testing second error message!");

    JsonArray multipleErrors = new JsonArray();
    multipleErrors.add(firstError.encode());
    multipleErrors.add(secondError.encode());

    HashMap<String, String> context = new HashMap<>();
    context.put(ITEM.value(), String.valueOf(multipleItems));
    context.put(INSTANCE.value(), instanceJson.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());
    context.put("ERRORS", String.valueOf(multipleErrors));


    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_ITEM_CREATED")
      .withContext(context);

    List<JournalRecord> journalRecords = JournalUtil.buildJournalRecordsByEvent(eventPayload,
      CREATE, ITEM, COMPLETED);

    Assert.assertNotNull(journalRecords);
    Assert.assertEquals(3, journalRecords.size());

    Assert.assertEquals(snapshotId, journalRecords.get(0).getJobExecutionId());
    Assert.assertEquals(recordId, journalRecords.get(0).getSourceId());
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
    Assert.assertEquals(recordId, journalRecords.get(1).getSourceId());
    Assert.assertEquals(1, journalRecords.get(1).getSourceRecordOrder().intValue());
    Assert.assertEquals(ITEM, journalRecords.get(1).getEntityType());
    Assert.assertEquals(firstErrorUUID, journalRecords.get(1).getEntityId());
    Assert.assertEquals(CREATE, journalRecords.get(1).getActionType());
    Assert.assertEquals(COMPLETED, journalRecords.get(1).getActionStatus());
    Assert.assertEquals("Testing first error message!", journalRecords.get(1).getError());

    Assert.assertEquals(snapshotId, journalRecords.get(2).getJobExecutionId());
    Assert.assertEquals(recordId, journalRecords.get(2).getSourceId());
    Assert.assertEquals(1, journalRecords.get(2).getSourceRecordOrder().intValue());
    Assert.assertEquals(ITEM, journalRecords.get(2).getEntityType());
    Assert.assertEquals(secondErrorUUID, journalRecords.get(2).getEntityId());
    Assert.assertEquals(CREATE, journalRecords.get(2).getActionType());
    Assert.assertEquals(COMPLETED, journalRecords.get(2).getActionStatus());
    Assert.assertEquals("Testing second error message!", journalRecords.get(2).getError());

    Assert.assertNotNull(journalRecords.get(1).getActionDate());

  }
}
