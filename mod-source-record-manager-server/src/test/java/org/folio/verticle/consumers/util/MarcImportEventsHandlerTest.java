package org.folio.verticle.consumers.util;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_COMPLETED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INCOMING_MARC_BIB_FOR_ORDER_PARSED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDING_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDING_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_ITEM_MATCHED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_ITEM_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ORDER_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ORDER_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_PENDING_ORDER_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INCOMING_MARC_BIB_RECORD_PARSED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.services.JournalRecordService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.marc4j.MarcJsonWriter;
import org.marc4j.marc.MarcFactory;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import org.folio.DataImportEventPayload;
import org.folio.ParsedRecord;
import org.folio.Record;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.services.MappingRuleCache;
import org.folio.services.journal.JournalRecordMapperException;
import org.folio.services.journal.JournalService;

@RunWith(VertxUnitRunner.class)
public class MarcImportEventsHandlerTest {

  private static final String TEST_TENANT = "tenant";

  private final MarcFactory marcFactory = MarcFactory.newInstance();

  @Captor
  private ArgumentCaptor<JsonArray> journalRecordCaptor;
  @Mock
  private MappingRuleCache mappingRuleCache;
  @Mock
  private JournalService journalService;
  @Mock
  private JournalRecordService journalRecordService;
  private MarcImportEventsHandler handler;

  private AutoCloseable mocks;

  @Before
  public void setUp() {
    mocks = MockitoAnnotations.openMocks(this);
    handler = new MarcImportEventsHandler(mappingRuleCache, journalRecordService);
  }

  @After
  public void tearDown() throws Exception {
    mocks.close();
  }

  @Test
  public void testSaveAuthorityJournalRecordWithTitleFrom1XXField() throws JournalRecordMapperException {
    when(mappingRuleCache.get(any())).thenReturn(Future.succeededFuture(Optional.of(new JsonObject())));

    var marcRecord = marcFactory.newRecord();
    var expectedTitleStart = "Title start";
    var expectedTitleEnd = "title end";
    marcRecord.addVariableField(marcFactory.newDataField("035", '0', '0', "a", "35488"));
    marcRecord.addVariableField(marcFactory.newDataField("150", '0', '0', "a", expectedTitleStart, "b", expectedTitleEnd));

    var payload = constructAuthorityPayload(marcRecord);

    handler.handle(journalService, payload, TEST_TENANT);

    verify(journalService).saveBatch(journalRecordCaptor.capture(), eq(TEST_TENANT));
    var actualJournalRecord = journalRecordCaptor.getValue().getJsonObject(0).mapTo(JournalRecord.class);

    assertEquals(expectedTitleStart + " " + expectedTitleEnd, actualJournalRecord.getTitle());
  }

  @Test
  public void testSaveAuthorityJournalRecordWithoutTitleWhen1XXFieldIsNotExist() throws JournalRecordMapperException {
    when(mappingRuleCache.get(any())).thenReturn(Future.succeededFuture(Optional.of(new JsonObject())));

    var marcRecord = marcFactory.newRecord();
    var expectedTitleStart = "Title start";
    var expectedTitleEnd = "title end";
    marcRecord.addVariableField(marcFactory.newDataField("035", '0', '0', "a", "35488"));
    marcRecord.addVariableField(marcFactory.newDataField("150", '0', '0', "a", expectedTitleStart, "b", expectedTitleEnd));

    var payload = constructAuthorityPayload(marcRecord);

    handler.handle(journalService, payload, TEST_TENANT);

    verify(journalService).saveBatch(journalRecordCaptor.capture(), eq(TEST_TENANT));
    var actualJournalRecord = journalRecordCaptor.getValue().getJsonObject(0).mapTo(JournalRecord.class);

    assertEquals(expectedTitleStart + " " + expectedTitleEnd, actualJournalRecord.getTitle());
  }

  @Test
  public void testSaveItemWithTitle() throws JournalRecordMapperException {
    String title = "The Journal of ecclesiastical history.";
    String incomingRecordId = UUID.randomUUID().toString();
    when(mappingRuleCache.get(any())).thenReturn(Future.succeededFuture(Optional.of(new JsonObject(
      Map.of("245", List.of(
        Map.of("target", "title",
          "subfield", List.of("a"))
      ))
    ))));
    var marcRecord = marcFactory.newRecord();
    marcRecord.addVariableField(marcFactory.newDataField("245", '0', '0', "a", title));

    var payload = constructUpdateItemPayload(marcRecord);
    payload.getContext().put("INCOMING_RECORD_ID", incomingRecordId);
    handler.handle(journalService, payload, TEST_TENANT);

    verify(journalService).saveBatch(journalRecordCaptor.capture(), eq(TEST_TENANT));
    var actualJournalRecord = journalRecordCaptor.getValue().getJsonObject(0).mapTo(JournalRecord.class);

    assertEquals(title, actualJournalRecord.getTitle());
    assertEquals(incomingRecordId, actualJournalRecord.getSourceId());
  }

  @Test
  public void testSaveUpdateHoldingsWithTitle() throws JournalRecordMapperException {
    String title = "The Journal of ecclesiastical history.";
    when(mappingRuleCache.get(any())).thenReturn(Future.succeededFuture(Optional.of(new JsonObject(
      Map.of("245", List.of(
        Map.of("target", "title",
          "subfield", List.of("a"))
      ))
    ))));
    var marcRecord = marcFactory.newRecord();
    marcRecord.addVariableField(marcFactory.newDataField("245", '0', '0', "a", title));

    var payload = constructUpdateHoldingsPayload(marcRecord);
    handler.handle(journalService, payload, TEST_TENANT);

    verify(journalService).saveBatch(journalRecordCaptor.capture(), eq(TEST_TENANT));
    var actualJournalRecord = journalRecordCaptor.getValue().getJsonObject(0).mapTo(JournalRecord.class);

    assertEquals(title, actualJournalRecord.getTitle());
  }

  @Test
  public void testDoNotSaveJournalRecordWhenEventOrderReadyForPostprocessingAndLastEventIsNotInventoryEntitiesCreated() throws JournalRecordMapperException {
    when(mappingRuleCache.get(any())).thenReturn(Future.succeededFuture(Optional.of(new JsonObject())));

    var payload = new DataImportEventPayload()
      .withEventType(DI_ORDER_CREATED_READY_FOR_POST_PROCESSING.value())
      .withEventsChain(List.of(DI_PENDING_ORDER_CREATED.value()));

    handler.handle(journalService, payload, TEST_TENANT);

    Mockito.verifyNoInteractions(journalService);
  }

  @Test
  public void testShouldNotUpdateJournalRecordsIfPOLineWithErrorAndWithoutOrderId() throws JournalRecordMapperException {
    String title = "The Journal of ecclesiastical history.";
    when(mappingRuleCache.get(any())).thenReturn(Future.succeededFuture(Optional.of(new JsonObject(
      Map.of("245", List.of(
        Map.of("target", "title",
          "subfield", List.of("a"))
      ))
    ))));
    var marcRecord = marcFactory.newRecord();
    marcRecord.addVariableField(marcFactory.newDataField("245", '0', '0', "a", title));

    var payload = constructCreateErrorPOLinePayloadWithoutOrderId(marcRecord);
    handler.handle(journalService, payload, TEST_TENANT);

    verify(journalService).saveBatch(journalRecordCaptor.capture(), eq(TEST_TENANT));
    var actualJournalRecord = journalRecordCaptor.getValue().getJsonObject(0).mapTo(JournalRecord.class);
    verify(journalRecordService, times(0)).updateErrorJournalRecordsByOrderIdAndJobExecution(anyString(), anyString(), anyString(), anyString());

    assertNotNull(actualJournalRecord.getTitle());
  }

  @Test
  public void testShouldUpdateJournalRecordsIfPOLineWithErrorAndWithOrderId() throws JournalRecordMapperException {
    when(journalRecordService.updateErrorJournalRecordsByOrderIdAndJobExecution(any(String.class), any(String.class), any(String.class), eq(TEST_TENANT))).thenReturn(Future.succeededFuture(1));
    String title = "The Journal of ecclesiastical history.";
    when(mappingRuleCache.get(any())).thenReturn(Future.succeededFuture(Optional.of(new JsonObject(
      Map.of("245", List.of(
        Map.of("target", "title",
          "subfield", List.of("a"))
      ))
    ))));
    var marcRecord = marcFactory.newRecord();
    marcRecord.addVariableField(marcFactory.newDataField("245", '0', '0', "a", title));

    var payload = constructCreateErrorPOLinePayloadWithOrderId(marcRecord);
    handler.handle(journalService, payload, TEST_TENANT);

    verify(journalRecordService).updateErrorJournalRecordsByOrderIdAndJobExecution(anyString(), anyString(), anyString(), anyString());
    verify(journalService).saveBatch(journalRecordCaptor.capture(), eq(TEST_TENANT));
    verify(journalRecordService, times(1)).updateErrorJournalRecordsByOrderIdAndJobExecution(anyString(), anyString(), anyString(), anyString());

    var actualJournalRecord = journalRecordCaptor.getValue().getJsonObject(0).mapTo(JournalRecord.class);

    assertEquals(title, actualJournalRecord.getTitle());
  }

  @Test
  public void testTransformPOLineJournalRecord(TestContext context) {
    Async async = context.async();
    when(journalRecordService.updateErrorJournalRecordsByOrderIdAndJobExecution(any(String.class), any(String.class), any(String.class), eq(TEST_TENANT))).thenReturn(Future.succeededFuture(1));
    String title = "The Journal of ecclesiastical history.";
    when(mappingRuleCache.get(any())).thenReturn(Future.succeededFuture(Optional.of(new JsonObject(
      Map.of("245", List.of(
        Map.of("target", "title",
          "subfield", List.of("a"))
      ))
    ))));
    var marcRecord = marcFactory.newRecord();
    marcRecord.addVariableField(marcFactory.newDataField("245", '0', '0', "a", title));

    var payload = constructCreateErrorPOLinePayloadWithOrderId(marcRecord);
    handler.transform(journalService, payload, TEST_TENANT)
      .onComplete(ar -> {
        assertTrue(ar.succeeded());
        assertEquals(1, ar.result().size());
        var actualJournalRecord = ar.result().stream().findFirst().get();
        assertEquals(title, actualJournalRecord.getTitle());
        async.complete();
      });
  }

  @Test
  public void testTransformReturnEmptyListIfNoJournalParams(TestContext context) {
    Async async = context.async();

    var payload = new DataImportEventPayload()
      .withEventType(DI_COMPLETED.value())
      .withEventsChain(List.of("Test"));
    handler.transform(journalService, payload, TEST_TENANT)
      .onComplete(ar -> {
        assertTrue(ar.succeeded());
        assertEquals(0, ar.result().size());
        async.complete();
      });
  }


  @Test
  public void testSaveNonMatchHoldings() throws JournalRecordMapperException {
    String title = "The Journal of ecclesiastical history.";
    when(mappingRuleCache.get(any())).thenReturn(Future.succeededFuture(Optional.of(new JsonObject(
      Map.of("245", List.of(
        Map.of("target", "title",
          "subfield", List.of("a"))
      ))
    ))));
    var marcRecord = marcFactory.newRecord();
    marcRecord.addVariableField(marcFactory.newDataField("245", '0', '0', "a", title));

    var payload = constructMatchHoldingsPayload(marcRecord);
    handler.handle(journalService, payload, TEST_TENANT);

    verify(journalService).saveBatch(journalRecordCaptor.capture(), eq(TEST_TENANT));
    var actualJournalRecords = journalRecordCaptor.getValue();

    assertEquals(3, actualJournalRecords.size());
  }

  @Test
  public void testSaveNonMatchItems() throws JournalRecordMapperException {
    String title = "The Journal of ecclesiastical history.";
    when(mappingRuleCache.get(any())).thenReturn(Future.succeededFuture(Optional.of(new JsonObject(
      Map.of("245", List.of(
        Map.of("target", "title",
          "subfield", List.of("a"))
      ))
    ))));
    var marcRecord = marcFactory.newRecord();
    marcRecord.addVariableField(marcFactory.newDataField("245", '0', '0', "a", title));

    var payload = constructMatchItemsPayload(marcRecord);
    handler.handle(journalService, payload, TEST_TENANT);

    verify(journalService).saveBatch(journalRecordCaptor.capture(), eq(TEST_TENANT));
    var actualJournalRecords = journalRecordCaptor.getValue();

    assertEquals(5, actualJournalRecords.size());
  }

  @Test
  public void testSaveUpdateInstanceWithTitle() throws JournalRecordMapperException {
    String title = "The Journal of ecclesiastical history.";
    when(mappingRuleCache.get(any())).thenReturn(Future.succeededFuture(Optional.of(new JsonObject(
      Map.of("245", List.of(
        Map.of("target", "title",
          "subfield", List.of("a"))
      ))
    ))));
    var marcRecord = marcFactory.newRecord();
    marcRecord.addVariableField(marcFactory.newDataField("245", '0', '0', "a", title));

    var payload = constructUpdateInstancePayload(marcRecord);
    handler.handle(journalService, payload, TEST_TENANT);

    verify(journalService).saveBatch(journalRecordCaptor.capture(), eq(TEST_TENANT));
    var actualJournalRecord = journalRecordCaptor.getValue().getJsonObject(0).mapTo(JournalRecord.class);

    assertEquals(title, actualJournalRecord.getTitle());
  }

  @Test
  public void testSaveOrderJournalRecordWithTitleFromMarcRecordAccordingToInstanceMappingRules() throws JournalRecordMapperException {
    String subfieldATitleValue = "The Journal";
    String subfieldBTitleValue = "of ecclesiastical history.";
    String expectedTitle = "The Journal of ecclesiastical history.";
    when(mappingRuleCache.get(any())).thenReturn(Future.succeededFuture(Optional.of(new JsonObject()
      .put("245", JsonArray.of(new JsonObject()
        .put("target", "title")
        .put("subfield", JsonArray.of("a", "b")))))
    ));

    var marcRecord = marcFactory.newRecord();
    marcRecord.addVariableField(marcFactory.newDataField("245", '0', '0', "a", subfieldATitleValue));
    marcRecord.addVariableField(marcFactory.newDataField("245", '0', '0', "b", subfieldBTitleValue));

    var payload = constructOrderPayload(marcRecord);
    handler.handle(journalService, payload, TEST_TENANT);

    verify(journalService).saveBatch(journalRecordCaptor.capture(), eq(TEST_TENANT));
    var actualJournalRecord = journalRecordCaptor.getValue().getJsonObject(0).mapTo(JournalRecord.class);

    assertEquals(expectedTitle, actualJournalRecord.getTitle());
  }

  private DataImportEventPayload constructMatchHoldingsPayload(org.marc4j.marc.Record marcRecord) {
    var record = new Record()
      .withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(marcRecordToJsonContent(marcRecord)));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(JournalRecord.EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put("HOLDINGS","[{\"instanceId\":\"946c4945-b711-4e67-bfb9-83fa30be6332\",\"hrid\":\"ho001\",\"id\":\"946c4945-b711-4e67-bfb9-83fa37be6312\"},{\"instanceId\":\"946c4945-b711-4e67-bfb9-83fa30be6331\",\"hrid\":\"ho002\",\"id\":\"946c4945-b111-4e67-bfb9-83fa30be6312\"}]");
    payloadContext.put("NOT_MATCHED_NUMBER","3");
    return new DataImportEventPayload()
      .withEventsChain(List.of(DI_INCOMING_MARC_BIB_RECORD_PARSED.value()))
      .withEventType(DI_INVENTORY_HOLDING_MATCHED.value())
      .withContext(payloadContext);
  }

  private DataImportEventPayload constructMatchItemsPayload(org.marc4j.marc.Record marcRecord) {
    var record = new Record()
      .withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(marcRecordToJsonContent(marcRecord)));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(JournalRecord.EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put("HOLDINGS","{\"instanceId\":\"946c4945-b711-4e67-bfb9-83fa30be633c\",\"hrid\":\"ho001\",\"id\":\"946c4945-b711-4e67-bfb9-83fa30be6312\"}");
    payloadContext.put("ITEM","[{\"holdingsRecordId\":\"946c4945-b711-4e67-bfb9-83fa30be633c\",\"hrid\":\"it001\",\"id\":\"946c4945-b711-4e67-bfb9-83fa30be4312\"},{\"holdingsRecordId\":\"946c4945-b711-4e67-bfb9-83fa30be633b\",\"hrid\":\"it002\",\"id\":\"946c4945-b711-4e67-bfb9-83fa30be6312\"}]");
    payloadContext.put("NOT_MATCHED_NUMBER","5");
    return new DataImportEventPayload()
      .withEventsChain(List.of(DI_INCOMING_MARC_BIB_RECORD_PARSED.value()))
      .withEventType(DI_INVENTORY_ITEM_MATCHED.value())
      .withContext(payloadContext);
  }

  private DataImportEventPayload constructAuthorityPayload(org.marc4j.marc.Record marcRecord) {
    var record = new Record()
      .withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(marcRecordToJsonContent(marcRecord)));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(JournalRecord.EntityType.MARC_AUTHORITY.value(), Json.encode(record));
    return new DataImportEventPayload()
      .withEventType(DI_SRS_MARC_AUTHORITY_RECORD_CREATED.value())
      .withContext(payloadContext);
  }

  private DataImportEventPayload constructUpdateItemPayload(org.marc4j.marc.Record marcRecord) {
    var record = new Record()
      .withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(marcRecordToJsonContent(marcRecord)));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(JournalRecord.EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    return new DataImportEventPayload()
      .withEventsChain(List.of(DI_INVENTORY_ITEM_UPDATED.value(), DI_INVENTORY_ITEM_UPDATED.value()))
      .withEventType(DI_INVENTORY_ITEM_UPDATED.value())
      .withContext(payloadContext);
  }

  private DataImportEventPayload constructUpdateHoldingsPayload(org.marc4j.marc.Record marcRecord) {
    var record = new Record()
      .withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(marcRecordToJsonContent(marcRecord)));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(JournalRecord.EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    return new DataImportEventPayload()
      .withEventsChain(List.of(DI_INVENTORY_HOLDING_UPDATED.value(), DI_INVENTORY_HOLDING_UPDATED.value()))
      .withEventType(DI_COMPLETED.value())
      .withContext(payloadContext);
  }

  private DataImportEventPayload constructUpdateInstancePayload(org.marc4j.marc.Record marcRecord) {
    var record = new Record()
      .withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(marcRecordToJsonContent(marcRecord)));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(JournalRecord.EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    return new DataImportEventPayload()
      .withEventsChain(List.of(DI_INVENTORY_INSTANCE_UPDATED.value(), DI_INVENTORY_INSTANCE_UPDATED.value()))
      .withEventType(DI_COMPLETED.value())
      .withContext(payloadContext);
  }


  private DataImportEventPayload constructCreateErrorPOLinePayloadWithoutOrderId(org.marc4j.marc.Record marcRecord) {
    var record = new Record()
      .withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(marcRecordToJsonContent(marcRecord)));
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(JournalRecord.EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put("ERROR", "Testing error");
    return new DataImportEventPayload()
      .withEventsChain(List.of(DI_ORDER_CREATED.value()))
      .withEventType(DI_ERROR.value())
      .withContext(payloadContext);
  }

  private DataImportEventPayload constructOrderPayload(org.marc4j.marc.Record marcRecord) {
    var record = new Record()
      .withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(marcRecordToJsonContent(marcRecord)))
      .withSnapshotId(UUID.randomUUID().toString());
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(JournalRecord.EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put("PO_LINE", "{\"purchaseOrderId\":\"946c4945-b711-4e67-bfb9-83fa30be633c\", \"titleOrPackage\":\"The Journal\"}");
    return new DataImportEventPayload()
      .withEventsChain(List.of(DI_INCOMING_MARC_BIB_FOR_ORDER_PARSED.value(), DI_ORDER_CREATED.value()))
      .withEventType(DI_COMPLETED.value())
      .withContext(payloadContext)
      .withJobExecutionId(UUID.randomUUID().toString());
  }

  private DataImportEventPayload constructCreateErrorPOLinePayloadWithOrderId(org.marc4j.marc.Record marcRecord) {
    var record = new Record()
      .withId(UUID.randomUUID().toString())
      .withParsedRecord(new ParsedRecord().withContent(marcRecordToJsonContent(marcRecord)))
      .withSnapshotId(UUID.randomUUID().toString());
    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(JournalRecord.EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    payloadContext.put("ERROR", "Testing error");
    payloadContext.put("PO_LINE", "{\"purchaseOrderId\":\"946c4945-b711-4e67-bfb9-83fa30be633c\"}");
    return new DataImportEventPayload()
      .withEventsChain(List.of(DI_ORDER_CREATED.value()))
      .withEventType(DI_ERROR.value())
      .withContext(payloadContext)
      .withJobExecutionId(UUID.randomUUID().toString());
  }

  private String marcRecordToJsonContent(org.marc4j.marc.Record marcRecord) {
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      MarcJsonWriter jsonWriter = new MarcJsonWriter(os);
      jsonWriter.write(marcRecord);
      return os.toString(StandardCharsets.UTF_8);
    } catch (IOException e) {
      return null;
    }
  }
}
