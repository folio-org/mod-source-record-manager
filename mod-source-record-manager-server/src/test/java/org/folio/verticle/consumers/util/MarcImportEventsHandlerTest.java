package org.folio.verticle.consumers.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.folio.DataImportEventTypes.DI_SRS_MARC_AUTHORITY_RECORD_CREATED;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Optional;
import java.util.UUID;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.marc4j.MarcJsonWriter;
import org.marc4j.marc.MarcFactory;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
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
  private MarcImportEventsHandler handler;

  private AutoCloseable mocks;

  @Before
  public void setUp() throws IOException {
    mocks = MockitoAnnotations.openMocks(this);
    handler = new MarcImportEventsHandler(mappingRuleCache);
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