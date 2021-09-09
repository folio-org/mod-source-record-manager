package org.folio.services;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.folio.dataimport.util.marc.MarcRecordAnalyzer;
import org.folio.dataimport.util.marc.MarcRecordType;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.RecordType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.HashMap;
import java.util.Map;

import static org.folio.rest.jaxrs.model.EntityType.EDIFACT_INVOICE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.EntityType.MARC_HOLDINGS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DataImportPayloadContextBuilderImplTest {

  @Mock
  private MarcRecordAnalyzer marcRecordAnalyzer;
  @InjectMocks
  private DataImportPayloadContextBuilderImpl builder;

  private Record record;
  private JsonObject rules;
  private MappingParameters params;


  @Before
  public void setUp() throws Exception {
    record = new Record();
    rules = new JsonObject();
    params = new MappingParameters();
  }

  @Test
  public void shouldBuildContextForMarcAuthorityRecord() {
    ParsedRecord parsedRecord = parsedRecord("{\"leader\":\"authority\"}");
    record.setRecordType(RecordType.MARC_AUTHORITY);
    record.setParsedRecord(parsedRecord);

    when(marcRecordAnalyzer.process(toJson(parsedRecord))).thenReturn(MarcRecordType.AUTHORITY);

    HashMap<String, String> context = builder.buildFrom(record);

    assertEquals(Map.of(MARC_AUTHORITY.value(), Json.encode(record)), context);
  }

  @Test
  public void shouldBuildContextForMarcBibRecord() {
    ParsedRecord parsedRecord = parsedRecord("{\"leader\":\"bibliographic\"}");
    record.setRecordType(Record.RecordType.MARC_BIB);
    record.setParsedRecord(parsedRecord);

    when(marcRecordAnalyzer.process(toJson(parsedRecord))).thenReturn(MarcRecordType.BIB);

    HashMap<String, String> context = builder.buildFrom(record);

    assertEquals(Map.of(
        MARC_BIBLIOGRAPHIC.value(), Json.encode(record)),
        context);
  }

  @Test
  public void shouldBuildContextForMarcHoldingRecord() {
    ParsedRecord parsedRecord = parsedRecord("{\"leader\":\"holding\"}");
    record.setRecordType(RecordType.MARC_HOLDING);
    record.setParsedRecord(parsedRecord);

    when(marcRecordAnalyzer.process(toJson(parsedRecord))).thenReturn(MarcRecordType.HOLDING);

    HashMap<String, String> context = builder.buildFrom(record);

    assertEquals(Map.of(
        MARC_HOLDINGS.value(), Json.encode(record)),
        context);
  }

  @Test
  public void shouldBuildContextForEdifactInvoice() {
    record.setRecordType(Record.RecordType.EDIFACT);

    HashMap<String, String> context = builder.buildFrom(record);

    assertEquals(Map.of(
        EDIFACT_INVOICE.value(), Json.encode(record)),
        context);
  }

  @Test
  public void shouldThrowNPEIfParsedRecordIsNullAndRecordIsMarc() {
    record.setRecordType(Record.RecordType.MARC_BIB);

    assertThrows("Parsed record is null", NullPointerException.class,
        () -> builder.buildFrom(record));
  }

  @Test
  public void shouldThrowNPEIfParsedRecordContentIsNullAndRecordIsMarc() {
    record.setRecordType(Record.RecordType.MARC_BIB);
    record.setParsedRecord(new ParsedRecord());

    assertThrows("Parsed record content is null", NullPointerException.class,
        () -> builder.buildFrom(record));
  }

  @Test
  public void shouldThrowExceptionIfMarcTypeUnsupported() {
    ParsedRecord parsedRecord = parsedRecord("{\"leader\":\"NA\"}");
    record.setRecordType(RecordType.MARC_BIB);
    record.setParsedRecord(parsedRecord);

    when(marcRecordAnalyzer.process(toJson(parsedRecord))).thenReturn(MarcRecordType.NA);

    assertThrows("Unsupported Marc record type", IllegalStateException.class,
        () -> builder.buildFrom(record));
  }

  private static ParsedRecord parsedRecord(String content) {
    return new ParsedRecord().withContent(content);
  }

  private static JsonObject toJson(ParsedRecord parsedRecord) {
    return new JsonObject(parsedRecord.getContent().toString());
  }

}
