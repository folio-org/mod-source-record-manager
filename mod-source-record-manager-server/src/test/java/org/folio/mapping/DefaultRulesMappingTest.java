package org.folio.mapping;

import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.util.List;
import org.folio.CallNumberType;
import org.folio.IdentifierType;
import org.folio.TestUtil;
import org.folio.processing.mapping.defaultmapper.RecordMapper;
import org.folio.processing.mapping.defaultmapper.RecordMapperBuilder;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.junit.Assert;
import org.junit.Test;

public class DefaultRulesMappingTest {

  private static final String RECORDS_PATH = "src/test/resources/org/folio/mapping/";
  private static final String DEFAULT_RULES_PATH = "src/main/resources/rules/";

  private RecordMapper<?> mapper;

  @Test
  public void testMarcToInstance() throws IOException {
    mapper = RecordMapperBuilder.buildMapper("MARC_BIB");
    JsonObject parsedRecord = readJson(RECORDS_PATH + "parsedBibRecord.json");
    JsonObject expectedMappedInstance = readJson(RECORDS_PATH + "mappedBibRecord.json");
    JsonObject mappingRules = readJson(DEFAULT_RULES_PATH + "marc_bib_rules.json");


    var actualInstance = mapper.mapRecord(parsedRecord, new MappingParameters(), mappingRules);
    Assert.assertEquals(expectedMappedInstance.encode(), JsonObject.mapFrom(actualInstance).put("id", "0").encode());
  }

  @Test
  public void testMarcToHoldings() throws IOException {
    mapper = RecordMapperBuilder.buildMapper("MARC_HOLDINGS");
    JsonObject parsedRecord = readJson(RECORDS_PATH + "parsedHoldingsRecord.json");
    JsonObject expectedMappedHoldings = readJson(RECORDS_PATH + "mappedHoldingsRecord.json");
    JsonObject mappingRules = readJson(DEFAULT_RULES_PATH + "marc_holdings_rules.json");


    var actualHoldings = mapper.mapRecord(parsedRecord, new MappingParameters()
      .withCallNumberTypes(getCallNumberTypeRefs()), mappingRules);
    Assert.assertEquals(expectedMappedHoldings.encode(), JsonObject.mapFrom(actualHoldings).put("id", "0").encode());
  }

  @Test
  public void testMarcToAuthority() throws IOException {
    mapper = RecordMapperBuilder.buildMapper("MARC_AUTHORITY");
    JsonObject parsedRecord = readJson(RECORDS_PATH + "parsedAuthorityRecord.json");
    JsonObject expectedMappedAuthority = readJson(RECORDS_PATH + "mappedAuthorityRecord.json");
    JsonObject mappingRules = readJson(DEFAULT_RULES_PATH + "marc_authority_rules.json");


    var actualAuthority = mapper.mapRecord(parsedRecord, new MappingParameters(), mappingRules);
    Assert.assertEquals(expectedMappedAuthority.encode(), JsonObject.mapFrom(actualAuthority).encode());
  }

  @Test
  public void testMarcToAuthorityLCCN() throws IOException {
    mapper = RecordMapperBuilder.buildMapper("MARC_AUTHORITY");
    JsonObject parsedRecord = readJson(RECORDS_PATH + "parsedAuthorityRecordLCCN.json");
    JsonObject expectedMappedAuthority = readJson(RECORDS_PATH + "mappedAuthorityRecordLCCN.json");
    JsonObject mappingRules = readJson(DEFAULT_RULES_PATH + "marc_authority_rules.json");
    var identifierType1 = new IdentifierType().withName("LCCN").withId("LCCN_identifierTypeId");
    var identifierType2 = new IdentifierType().withName("Cancelled LCCN").withId("CancelledLCCN_identifierTypeId");
    var mappingParameters = new MappingParameters().withIdentifierTypes(List.of(identifierType1, identifierType2));

    var actualAuthority = mapper.mapRecord(parsedRecord, mappingParameters, mappingRules);
    Assert.assertEquals(expectedMappedAuthority.encode(), JsonObject.mapFrom(actualAuthority).encode());
  }


  private List<CallNumberType> getCallNumberTypeRefs(){
    return List.of(
      new CallNumberType().withName("Library of Congress classification")
        .withId("95467209-6d7b-468b-94df-0f5d7ad2747d")
    );
  }

  private JsonObject readJson(String filePath) throws IOException {
    return new JsonObject(TestUtil.readFileFromPath(filePath));
  }
}
