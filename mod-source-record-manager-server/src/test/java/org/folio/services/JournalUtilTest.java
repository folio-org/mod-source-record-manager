package org.folio.services;

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

    List<JournalRecord>  journalRecord = JournalUtil.buildJournalRecordByEvent(eventPayload,
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

    JournalUtil.buildJournalRecordByEvent(eventPayload,
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

    List<JournalRecord> journalRecord = JournalUtil.buildJournalRecordByEvent(eventPayload,
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

 /* @Test
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

    HashMap<String, String> context = new HashMap<>();
    context.put(HOLDINGS.value(), holdingsJson.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_HOLDING_CREATED")
      .withContext(context);

    JournalRecord journalRecord = JournalUtil.buildJournalRecordByEvent(eventPayload,
      CREATE, HOLDINGS, COMPLETED);

    Assert.assertNotNull(journalRecord);
    Assert.assertEquals(snapshotId, journalRecord.getJobExecutionId());
    Assert.assertEquals(recordId, journalRecord.getSourceId());
    Assert.assertEquals(1, journalRecord.getSourceRecordOrder().intValue());
    Assert.assertEquals(HOLDINGS, journalRecord.getEntityType());
    Assert.assertEquals(holdingsId, journalRecord.getEntityId());
    Assert.assertEquals(holdingsHrid, journalRecord.getEntityHrId());
    Assert.assertEquals(instanceId, journalRecord.getInstanceId());
    Assert.assertEquals(CREATE, journalRecord.getActionType());
    Assert.assertEquals(COMPLETED, journalRecord.getActionStatus());
    Assert.assertNotNull(journalRecord.getActionDate());
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

    HashMap<String, String> context = new HashMap<>();
    context.put(ITEM.value(), itemJson.encode());
    context.put(INSTANCE.value(), instanceJson.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_ITEM_CREATED")
      .withContext(context);

    JournalRecord journalRecord = JournalUtil.buildJournalRecordByEvent(eventPayload,
      CREATE, ITEM, COMPLETED);

    Assert.assertNotNull(journalRecord);
    Assert.assertEquals(snapshotId, journalRecord.getJobExecutionId());
    Assert.assertEquals(recordId, journalRecord.getSourceId());
    Assert.assertEquals(1, journalRecord.getSourceRecordOrder().intValue());
    Assert.assertEquals(ITEM, journalRecord.getEntityType());
    Assert.assertEquals(itemId, journalRecord.getEntityId());
    Assert.assertEquals(itemHrid, journalRecord.getEntityHrId());
    Assert.assertEquals(instanceId, journalRecord.getInstanceId());
    Assert.assertEquals(holdingsId, journalRecord.getHoldingsId());
    Assert.assertEquals(CREATE, journalRecord.getActionType());
    Assert.assertEquals(COMPLETED, journalRecord.getActionStatus());
    Assert.assertNotNull(journalRecord.getActionDate());
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

    HashMap<String, String> context = new HashMap<>();
    context.put(ITEM.value(), itemJson.encode());
    context.put(HOLDINGS.value(), holdingsJson.encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), recordJson.encode());

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_ITEM_CREATED")
      .withContext(context);

    JournalRecord journalRecord = JournalUtil.buildJournalRecordByEvent(eventPayload,
      CREATE, ITEM, COMPLETED);

    Assert.assertNotNull(journalRecord);
    Assert.assertEquals(snapshotId, journalRecord.getJobExecutionId());
    Assert.assertEquals(recordId, journalRecord.getSourceId());
    Assert.assertEquals(1, journalRecord.getSourceRecordOrder().intValue());
    Assert.assertEquals(ITEM, journalRecord.getEntityType());
    Assert.assertEquals(itemId, journalRecord.getEntityId());
    Assert.assertEquals(itemHrid, journalRecord.getEntityHrId());
    Assert.assertEquals(instanceId, journalRecord.getInstanceId());
    Assert.assertEquals(holdingsId, journalRecord.getHoldingsId());
    Assert.assertEquals(CREATE, journalRecord.getActionType());
    Assert.assertEquals(COMPLETED, journalRecord.getActionStatus());
    Assert.assertNotNull(journalRecord.getActionDate());
  }*/

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

    List<JournalRecord> journalRecord = JournalUtil.buildJournalRecordByEvent(eventPayload,
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

    List<JournalRecord> journalRecord = JournalUtil.buildJournalRecordByEvent(eventPayload,
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

    List<JournalRecord> journalRecord = JournalUtil.buildJournalRecordByEvent(eventPayload,
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
}
