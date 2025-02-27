package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.Record;
import org.folio.services.MappingRuleService;

public class AuthorityMappingGeneralSubdivisionCustomMigration extends BaseMappingRulesMigration {

  protected AuthorityMappingGeneralSubdivisionCustomMigration(MappingRuleService mappingRuleService) {
    super(mappingRuleService);
  }

  @Override
  protected Record.RecordType getRecordType() {
    return MARC_AUTHORITY;
  }

  @Override
  protected String updateRules(JsonObject rules) {
    var subfields = JsonArray.of("v", "x", "y", "z");
    var emptyRules = new JsonArray();
    addFieldIfNotExists(rules, "180",
      createField("generalSubdivision", "Heading general subdivision", subfields, emptyRules));
    addFieldIfNotExists(rules, "480",
      createField("sftGeneralSubdivision", "See from tracing general subdivision", subfields, emptyRules));
    addFieldIfNotExists(rules, "580",
      createField("saftGeneralSubdivision", "See also from tracing general subdivision", subfields, emptyRules));
    return sortRules(rules).encode();
  }

  @Override
  public String getFeatureVersion() {
    return "3.10.0";
  }

  @Override
  public String getDescription() {
    return "Authority mapping rules: add rules for general subdivision fields";
  }
}
