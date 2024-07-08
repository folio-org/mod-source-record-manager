package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.List;
import org.folio.Record;
import org.folio.services.MappingRuleService;
import org.springframework.stereotype.Component;

@Component
public class AuthorityMappingNameSubjectMetadataCustomMigration extends BaseMappingRulesMigration {

  public AuthorityMappingNameSubjectMetadataCustomMigration(MappingRuleService mappingRuleService) {
    super(mappingRuleService);
  }

  @Override
  protected Record.RecordType getRecordType() {
    return MARC_AUTHORITY;
  }

  @Override
  public String getFeatureVersion() {
    return "3.9.0";
  }

  @Override
  public String getDescription() {
    return "Authority mapping rules: update rules for name fields with subject metadata";
  }

  @Override
  protected String updateRules(JsonObject rules) {
    List.of("100", "110", "111", "400", "410", "411", "500", "510", "511")
      .forEach(tag -> updateRulesForTag(tag, rules));
    return rules.encode();
  }

  private void updateRulesForTag(String tag, JsonObject rules) {
    var tagRules = rules.getJsonArray(tag);
    if (tagRules == null) {
      return;
    }
    for (int i = 0; i < tagRules.size(); i++) {
      var rule = tagRules.getJsonObject(i);
      if (!rule.getString("target").endsWith("Title")) {
        var subfields = rule.getJsonArray("subfield");
        var newSubfields = JsonArray.of("v", "x", "y", "z");
        subfields.addAll(newSubfields);
      }
    }

  }
}
