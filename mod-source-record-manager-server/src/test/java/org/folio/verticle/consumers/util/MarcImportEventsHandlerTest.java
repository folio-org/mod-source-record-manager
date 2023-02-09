package org.folio.verticle.consumers.util;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_COMPLETED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_HOLDING_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_INVENTORY_ITEM_UPDATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ORDER_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ORDER_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_PENDING_ORDER_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_CREATED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.util.concurrent.CompletableFuture;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
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
  private ArgumentCaptor<JsonObject> journalRecordCaptor;
  @Mock
  private MappingRuleCache mappingRuleCache;
  @Mock
  private JournalService journalService;
  @Mock
  private JournalRecordService journalRecordService;
  private MarcImportEventsHandler handler;

  private AutoCloseable mocks;

  @Before
  public void setUp() throws IOException {
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

    verify(journalService).save(journalRecordCaptor.capture(), eq(TEST_TENANT));
    var actualJournalRecord = journalRecordCaptor.getValue().mapTo(JournalRecord.class);

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

    verify(journalService).save(journalRecordCaptor.capture(), eq(TEST_TENANT));
    var actualJournalRecord = journalRecordCaptor.getValue().mapTo(JournalRecord.class);

    assertEquals(expectedTitleStart + " " + expectedTitleEnd, actualJournalRecord.getTitle());
  }

  @Test
  public void testSaveItemWithTitle() throws JournalRecordMapperException {
    String title = "The Journal of ecclesiastical history.";
    when(mappingRuleCache.get(any())).thenReturn(Future.succeededFuture(Optional.of(new JsonObject(
      Map.of("245", List.of(
        Map.of("target", "title",
          "subfield", List.of("a"))
      ))
    ))));
    var marcRecord = marcFactory.newRecord();
    marcRecord.addVariableField(marcFactory.newDataField("245", '0', '0', "a", title));

    var payload = constructUpdateItemPayload(marcRecord);
    handler.handle(journalService, payload, TEST_TENANT);

    verify(journalService).save(journalRecordCaptor.capture(), eq(TEST_TENANT));
    var actualJournalRecord = journalRecordCaptor.getValue().mapTo(JournalRecord.class);

    assertEquals(title, actualJournalRecord.getTitle());
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

    verify(journalService).save(journalRecordCaptor.capture(), eq(TEST_TENANT));
    var actualJournalRecord = journalRecordCaptor.getValue().mapTo(JournalRecord.class);

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

    verify(journalService).save(journalRecordCaptor.capture(), eq(TEST_TENANT));
    var actualJournalRecord = journalRecordCaptor.getValue().mapTo(JournalRecord.class);
    verify(journalRecordService, times(0)).updateErrorJournalRecordsByOrderIdAndJobExecution(anyString(), anyString(), anyString(), anyString());

    assertNull(actualJournalRecord.getTitle());
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
    verify(journalService).save(journalRecordCaptor.capture(), eq(TEST_TENANT));
    verify(journalRecordService, times(1)).updateErrorJournalRecordsByOrderIdAndJobExecution(anyString(), anyString(), anyString(), anyString());

    var actualJournalRecord = journalRecordCaptor.getValue().mapTo(JournalRecord.class);

    assertNull(actualJournalRecord.getTitle());
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
