package org.folio.services;

import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;

import java.util.HashMap;
import java.util.UUID;

import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.services.journal.JournalRecordMapperException;
import org.folio.services.journal.JournalUtil;
import org.junit.Assert;
import org.junit.Test;

import io.vertx.core.json.JsonObject;

public class JournalUtilTest {

  @Test
  public void shouldBuildJournalRecord() throws JournalRecordMapperException {
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

    JournalRecord journalRecord = JournalUtil.buildJournalRecordByEvent(eventPayload,
      JournalRecord.ActionType.CREATE, JournalRecord.EntityType.INSTANCE, JournalRecord.ActionStatus.COMPLETED);

    Assert.assertNotNull(journalRecord);
    Assert.assertEquals(snapshotId, journalRecord.getJobExecutionId());
    Assert.assertEquals(recordId, journalRecord.getSourceId());
    Assert.assertEquals(1, journalRecord.getSourceRecordOrder().intValue());
    Assert.assertEquals(JournalRecord.EntityType.INSTANCE, journalRecord.getEntityType());
    Assert.assertEquals(instanceId, journalRecord.getEntityId());
    Assert.assertEquals(instanceHrid, journalRecord.getEntityHrId());
    Assert.assertEquals(JournalRecord.ActionType.CREATE, journalRecord.getActionType());
    Assert.assertEquals(JournalRecord.ActionStatus.COMPLETED, journalRecord.getActionStatus());
    Assert.assertNotNull(journalRecord.getActionDate());
  }

  @Test(expected = JournalRecordMapperException.class)
  public void shouldThrowExceptionIfInstanceIsNotExists() throws JournalRecordMapperException {
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

    JournalUtil.buildJournalRecordByEvent(eventPayload,
      JournalRecord.ActionType.CREATE, JournalRecord.EntityType.INSTANCE, JournalRecord.ActionStatus.COMPLETED);
  }

  @Test(expected = JournalRecordMapperException.class)
  public void shouldThrowExceptionIfRecordIsNotExists() throws JournalRecordMapperException {
    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = UUID.randomUUID().toString();

    JsonObject instanceJson = new JsonObject()
      .put("id", instanceId)
      .put("hrid", instanceHrid);

    HashMap<String, String> context = new HashMap<>();
    context.put(INSTANCE.value(), instanceJson.encode());

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType("DI_INVENTORY_INSTANCE_CREATED")
      .withContext(context);

    JournalUtil.buildJournalRecordByEvent(eventPayload,
      JournalRecord.ActionType.CREATE, JournalRecord.EntityType.INSTANCE, JournalRecord.ActionStatus.COMPLETED);
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
      JournalRecord.ActionType.CREATE, JournalRecord.EntityType.INSTANCE, JournalRecord.ActionStatus.COMPLETED);
  }
}
