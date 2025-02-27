package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.Record;
import org.folio.services.MappingRuleService;
import org.springframework.stereotype.Component;

@Component
public class AuthorityMappingNamedEventCustomMigration extends BaseMappingRulesMigration {

  protected AuthorityMappingNamedEventCustomMigration(MappingRuleService mappingRuleService) {
    super(mappingRuleService);
  }

  @Override
  protected Record.RecordType getRecordType() {
    return MARC_AUTHORITY;
  }

  @Override
  protected String updateRules(JsonObject rules) {
    var subfields = JsonArray.of("a", "c", "d", "g", "v", "x", "y", "z");
    var emptyRules = new JsonArray();
    addFieldIfNotExists(rules, "147", createField("namedEvent", "Heading named event", subfields, emptyRules));
    addFieldIfNotExists(rules, "447",
      createField("sftNamedEvent", "See from tracing named event", subfields, emptyRules));
    addFieldIfNotExists(rules, "547",
      createField("saftNamedEvent", "See also from tracing named event", subfields, emptyRules));
    return sortRules(rules).encode();
  }

  @Override
  public String getFeatureVersion() {
    return "3.10.0";
  }

  @Override
  public String getDescription() {
    return "Authority mapping rules: add rules for named event fields";
  }
}
