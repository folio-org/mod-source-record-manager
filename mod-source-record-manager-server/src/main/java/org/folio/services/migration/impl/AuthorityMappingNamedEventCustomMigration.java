package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.Map;
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
    addFieldIfNotExists(rules, "147", createField("namedEvent", "Heading named event"));
    addFieldIfNotExists(rules, "447", createField("sftNamedEvent", "See from tracing named event"));
    addFieldIfNotExists(rules, "547", createField("saftNamedEvent", "See also from tracing named event"));

    return rules.stream()
      .sorted(Map.Entry.comparingByKey())
      .collect(JsonObject::new, (json, entry) -> json.put(entry.getKey(), entry.getValue()), JsonObject::mergeIn)
      .encode();
  }

  @Override
  public String getFeatureVersion() {
    return "3.10.0";
  }

  @Override
  public String getDescription() {
    return "Authority mapping rules: add rules for named event fields";
  }

  private void addFieldIfNotExists(JsonObject rules, String tag, JsonObject field) {
    if (!rules.containsKey(tag)) {
      rules.put(tag, JsonArray.of(field));
    }
  }

  private JsonObject createField(String target, String description) {
    return new JsonObject()
      .put("target", target)
      .put("description", description)
      .put("subfield", JsonArray.of("a", "c", "d", "g", "v", "x", "y", "z"))
      .put("rules", new JsonArray());
  }
}
