package org.folio.verticle.consumers.util;

import static org.folio.DataImportEventTypes.DI_COMPLETED;
import static org.folio.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDING_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDING_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDING_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_ITEM_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_ITEM_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_ITEM_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_LOG_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_LOG_SRS_MARC_BIB_RECORD_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_NOT_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_HOLDING_RECORD_CREATED;

import java.util.Collections;
import java.util.HashMap;
import java.util.Optional;

import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.JournalRecord;

@RunWith(BlockJUnit4ClassRunner.class)
public class JournalParamsTest {

  public DataImportEventPayload eventPayload;
  public HashMap<String, String> context;

  @Before
  public void setUp() {
    eventPayload = new DataImportEventPayload();
    context = new HashMap<>();
  }

  @Test
  public void shouldPopulateEntityTypeMarcHoldingsWhenEventTypeIsDiError() {
    eventPayload.setEventType(DI_ERROR.value());
    context.put(EntityType.MARC_HOLDINGS.value(), new JsonObject().encode());
    eventPayload.setContext(context);

    var journalParamsOptional =
      JournalParams.JournalParamsEnum.getValue(eventPayload.getEventType()).getJournalParams(eventPayload);

    var journalParams = journalParamsOptional.get();

    Assert.assertEquals(journalParams.journalEntityType, JournalRecord.EntityType.MARC_HOLDINGS);
    Assert.assertEquals(journalParams.journalActionType, JournalRecord.ActionType.CREATE);
    Assert.assertEquals(journalParams.journalActionStatus, JournalRecord.ActionStatus.ERROR);
  }

  @Test
  public void shouldPopulateEntityTypeMarcBibWhenEventTypeIsDiError() {
    eventPayload.setEventType(DI_ERROR.value());
    context.put(EntityType.MARC_BIBLIOGRAPHIC.value(), new JsonObject().encode());
    eventPayload.setContext(context);

    var journalParamsOptional =
      JournalParams.JournalParamsEnum.getValue(eventPayload.getEventType()).getJournalParams(eventPayload);

    var journalParams = journalParamsOptional.get();

    Assert.assertEquals(journalParams.journalEntityType, JournalRecord.EntityType.MARC_BIBLIOGRAPHIC);
    Assert.assertEquals(journalParams.journalActionType, JournalRecord.ActionType.CREATE);
    Assert.assertEquals(journalParams.journalActionStatus, JournalRecord.ActionStatus.ERROR);
  }

  @Test
  public void shouldPopulateEntityTypeEdifactWhenEventTypeIsDiError() {
    eventPayload.setEventType(DI_ERROR.value());
    context.put(EntityType.EDIFACT_INVOICE.value(), new JsonObject().encode());
    eventPayload.setContext(context);

    var journalParamsOptional =
      JournalParams.JournalParamsEnum.getValue(eventPayload.getEventType()).getJournalParams(eventPayload);

    var journalParams = journalParamsOptional.get();

    Assert.assertEquals(journalParams.journalEntityType, JournalRecord.EntityType.EDIFACT);
    Assert.assertEquals(journalParams.journalActionType, JournalRecord.ActionType.CREATE);
    Assert.assertEquals(journalParams.journalActionStatus, JournalRecord.ActionStatus.ERROR);
  }

  @Test
  public void shouldPopulateEntityTypeMarcAuthorityWhenEventTypeIsDiError() {
    eventPayload.setEventType(DI_ERROR.value());
    context.put(EntityType.MARC_AUTHORITY.value(), new JsonObject().encode());
    eventPayload.setContext(context);

    var journalParamsOptional =
      JournalParams.JournalParamsEnum.getValue(eventPayload.getEventType()).getJournalParams(eventPayload);

    var journalParams = journalParamsOptional.get();
    Assert.assertEquals(journalParams.journalEntityType, JournalRecord.EntityType.MARC_AUTHORITY);
    Assert.assertEquals(journalParams.journalActionType, JournalRecord.ActionType.CREATE);
    Assert.assertEquals(journalParams.journalActionStatus, JournalRecord.ActionStatus.ERROR);
  }

  @Test
  public void shouldPopulateEntityTypeMarcHoldingsWhenEventChainIsNotEmpty() {
    eventPayload.setEventType(DI_ERROR.value());
    eventPayload.setEventsChain(Collections.singletonList("DI_SRS_MARC_HOLDING_RECORD_CREATED"));

    Optional<JournalParams> journalParamsOptional =
      JournalParams.JournalParamsEnum.getValue(eventPayload.getEventType()).getJournalParams(eventPayload);

    var journalParams = journalParamsOptional.get();

    Assert.assertEquals(journalParams.journalEntityType, JournalRecord.EntityType.MARC_HOLDINGS);
    Assert.assertEquals(journalParams.journalActionType, JournalRecord.ActionType.CREATE);
    Assert.assertEquals(journalParams.journalActionStatus, JournalRecord.ActionStatus.ERROR);
  }

  @Test
  public void shouldPopulateEntityTypeMarcAuthorityWhenEventTypeIsDiCompleted() {
    eventPayload.setEventType(DI_COMPLETED.value());
    context.put(EntityType.MARC_HOLDINGS.value(), new JsonObject().encode());
    eventPayload.setContext(context);
    eventPayload.setEventsChain(Collections.singletonList("DI_SRS_MARC_HOLDING_RECORD_CREATED"));

    var journalParamsOptional =
      JournalParams.JournalParamsEnum.getValue(eventPayload.getEventType()).getJournalParams(eventPayload);

    var journalParams = journalParamsOptional.get();
    Assert.assertEquals(journalParams.journalEntityType, JournalRecord.EntityType.MARC_HOLDINGS);
    Assert.assertEquals(journalParams.journalActionType, JournalRecord.ActionType.CREATE);
    Assert.assertEquals(journalParams.journalActionStatus, JournalRecord.ActionStatus.COMPLETED);
  }

  @Test
  public void shouldPopulateOptionalForIncorrectEventTypeWhenEventTypeIsDiCompleted() {
    eventPayload.setEventType(DI_COMPLETED.value());
    context.put(EntityType.MARC_HOLDINGS.value(), new JsonObject().encode());
    eventPayload.setContext(context);
    eventPayload.setEventsChain(Collections.singletonList("incorrect_event_type_name"));

    var journalParamsOptional =
      JournalParams.JournalParamsEnum.getValue(eventPayload.getEventType()).getJournalParams(eventPayload);

    Assert.assertEquals(journalParamsOptional, Optional.empty());
  }

  @Test
  public void shouldPopulateEntityTypeMarcHoldingsWhenEventTypeIsDiMarcHoldingRecordCreated() {
    populateEntityTypeAndActionTypeByEventType(DI_SRS_MARC_HOLDING_RECORD_CREATED, JournalRecord.EntityType.MARC_HOLDINGS, JournalRecord.ActionType.CREATE);
  }

  @Test
  public void shouldPopulateEntityTypeMarcBibWhenEventTypeIsDiLogSrsMarcBibRecordUpdated() {
    populateEntityTypeAndActionTypeByEventType(DI_LOG_SRS_MARC_BIB_RECORD_UPDATED, JournalRecord.EntityType.MARC_BIBLIOGRAPHIC, JournalRecord.ActionType.UPDATE);
  }

  @Test
  public void shouldPopulateEntityTypeMarcBibWhenEventTypeIsDiLogSrsMarcBibRecordCreated() {
    populateEntityTypeAndActionTypeByEventType(DI_LOG_SRS_MARC_BIB_RECORD_CREATED, JournalRecord.EntityType.MARC_BIBLIOGRAPHIC, JournalRecord.ActionType.CREATE);
  }

  @Test
  public void shouldPopulateEntityTypeMarcAuthorityWhenEventTypeIsDiSrsMarcAuthorityRecordCreated() {
    populateEntityTypeAndActionTypeByEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED, JournalRecord.EntityType.MARC_AUTHORITY, JournalRecord.ActionType.CREATE);
  }

  @Test
  public void shouldPopulateEntityTypeItemWhenEventTypeIsDiInventoryItemNonMatched() {
    populateEntityTypeAndActionTypeByEventType(DI_INVENTORY_ITEM_NOT_MATCHED, JournalRecord.EntityType.ITEM, JournalRecord.ActionType.NON_MATCH);
  }

  @Test
  public void shouldPopulateEntityTypeItemWhenEventTypeIsDiInventoryItemUpdated() {
    populateEntityTypeAndActionTypeByEventType(DI_INVENTORY_ITEM_UPDATED, JournalRecord.EntityType.ITEM, JournalRecord.ActionType.UPDATE);
  }

  @Test
  public void shouldPopulateEntityTypeItemWhenEventTypeIsDiInventoryItemCreated() {
    populateEntityTypeAndActionTypeByEventType(DI_INVENTORY_ITEM_CREATED, JournalRecord.EntityType.ITEM, JournalRecord.ActionType.CREATE);
  }

  @Test
  public void shouldPopulateEntityTypeHoldingsWhenEventTypeIsDiInventoryHoldingNotMarched() {
    populateEntityTypeAndActionTypeByEventType(DI_INVENTORY_HOLDING_NOT_MATCHED, JournalRecord.EntityType.HOLDINGS, JournalRecord.ActionType.NON_MATCH);
  }

  @Test
  public void shouldPopulateEntityTypeHoldingsWhenEventTypeIsDiInventoryHoldingUpdated() {
    populateEntityTypeAndActionTypeByEventType(DI_INVENTORY_HOLDING_UPDATED, JournalRecord.EntityType.HOLDINGS, JournalRecord.ActionType.UPDATE);
  }

  @Test
  public void shouldPopulateEntityTypeHoldingsWhenEventTypeIsDiInventoryHoldingCreated() {
    populateEntityTypeAndActionTypeByEventType(DI_INVENTORY_HOLDING_CREATED, JournalRecord.EntityType.HOLDINGS, JournalRecord.ActionType.CREATE);
  }

  @Test
  public void shouldPopulateEntityTypeInstanceWhenEventTypeIsDiInventoryInstanceNonMatched() {
    populateEntityTypeAndActionTypeByEventType(DI_INVENTORY_INSTANCE_NOT_MATCHED, JournalRecord.EntityType.INSTANCE, JournalRecord.ActionType.NON_MATCH);
  }

  @Test
  public void shouldPopulateEntityTypeInstanceWhenEventTypeIsDiInventoryInstanceUpdated() {
    populateEntityTypeAndActionTypeByEventType(DI_INVENTORY_INSTANCE_UPDATED, JournalRecord.EntityType.INSTANCE, JournalRecord.ActionType.UPDATE);
  }

  @Test
  public void shouldPopulateEntityTypeMarcBibWhenEventTypeIsDiSrsMarcBibReadyFOrPostProcessing() {
    populateEntityTypeAndActionTypeByEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED_READY_FOR_POST_PROCESSING, JournalRecord.EntityType.INSTANCE, JournalRecord.ActionType.UPDATE);
  }

  @Test
  public void shouldPopulateEntityTypeInstanceWhenEventTypeIsDiInventoryInstanceCreated() {
    populateEntityTypeAndActionTypeByEventType(DI_INVENTORY_INSTANCE_CREATED, JournalRecord.EntityType.INSTANCE, JournalRecord.ActionType.CREATE);
  }

  @Test
  public void shouldPopulateEntityTypeMarcBibWhenEventTypeIsDiSrsMarcBibRecordNotMatched() {
    populateEntityTypeAndActionTypeByEventType(DI_SRS_MARC_BIB_RECORD_NOT_MATCHED, JournalRecord.EntityType.MARC_BIBLIOGRAPHIC, JournalRecord.ActionType.NON_MATCH);
  }

  @Test
  public void shouldPopulateEntityTypeMarcBibWhenEventTypeIsDiSrsMarcBibRecordMatched() {
    populateEntityTypeAndActionTypeByEventType(DI_SRS_MARC_BIB_RECORD_MATCHED, JournalRecord.EntityType.MARC_BIBLIOGRAPHIC, JournalRecord.ActionType.MATCH);
  }

  @Test
  public void shouldPopulateEntityTypeMarcBibWhenEventTypeIsDiSrsMarcBibRecordModified() {
    populateEntityTypeAndActionTypeByEventType(DI_SRS_MARC_BIB_RECORD_MODIFIED, JournalRecord.EntityType.MARC_BIBLIOGRAPHIC, JournalRecord.ActionType.MODIFY);
  }

  @Test
  public void shouldPopulateEntityTypeMarcBibWhenEventTypeIsDiSrsMarcBibRecordUpdated() {
    populateEntityTypeAndActionTypeByEventType(DI_SRS_MARC_BIB_RECORD_UPDATED, JournalRecord.EntityType.MARC_BIBLIOGRAPHIC, JournalRecord.ActionType.UPDATE);
  }

  private void populateEntityTypeAndActionTypeByEventType(DataImportEventTypes eventType, JournalRecord.EntityType entityType, JournalRecord.ActionType actionType) {
    eventPayload.setEventType(eventType.value());

    var journalParamsOptional =
      JournalParams.JournalParamsEnum.getValue(eventPayload.getEventType()).getJournalParams(eventPayload);

    var journalParams = journalParamsOptional.get();

    Assert.assertEquals(journalParams.journalEntityType, entityType);
    Assert.assertEquals(journalParams.journalActionType, actionType);
    Assert.assertEquals(journalParams.journalActionStatus, JournalRecord.ActionStatus.COMPLETED);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionWhenJournalParamsNameIsNotAvailable() {
    JournalParams.JournalParamsEnum.getValue("not available event");
  }
}
