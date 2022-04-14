package org.folio.mapping;

import io.vertx.core.json.JsonObject;
import java.io.IOException;
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
    JsonObject expectedMappedAuthority = readJson(RECORDS_PATH + "mappedBibRecord.json");
    JsonObject mappingRules = readJson(DEFAULT_RULES_PATH + "marc_bib_rules.json");


    var actualAuthority = mapper.mapRecord(parsedRecord, new MappingParameters(), mappingRules);
    Assert.assertEquals(expectedMappedAuthority.encode(), JsonObject.mapFrom(actualAuthority).put("id", "0").encode());
  }

  @Test
  public void testMarcToHoldings() throws IOException {
    mapper = RecordMapperBuilder.buildMapper("MARC_HOLDINGS");
    JsonObject parsedRecord = readJson(RECORDS_PATH + "parsedHoldingsRecord.json");
    JsonObject expectedMappedAuthority = readJson(RECORDS_PATH + "mappedHoldingsRecord.json");
    JsonObject mappingRules = readJson(DEFAULT_RULES_PATH + "marc_holdings_rules.json");


    var actualAuthority = mapper.mapRecord(parsedRecord, new MappingParameters(), mappingRules);
    Assert.assertEquals(expectedMappedAuthority.encode(), JsonObject.mapFrom(actualAuthority).put("id", "0").encode());
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

  private JsonObject readJson(String filePath) throws IOException {
    return new JsonObject(TestUtil.readFileFromPath(filePath));
  }
}
