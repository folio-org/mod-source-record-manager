package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.List;
import org.folio.services.MappingRuleService;
import org.folio.services.migration.CustomMigration;
import org.springframework.stereotype.Component;

@Component
public class AuthorityMappingNameSubjectMetadataCustomMigration implements CustomMigration {

  private final MappingRuleService mappingRuleService;

  public AuthorityMappingNameSubjectMetadataCustomMigration(MappingRuleService mappingRuleService) {
    this.mappingRuleService = mappingRuleService;
  }

  @Override
  public Future<Void> migrate(String tenantId) {
    return mappingRuleService.get(MARC_AUTHORITY, tenantId)
      .compose(rules -> {
        if (rules.isPresent()) {
          var newRules = updateRules(rules.get());
          return mappingRuleService.internalUpdate(newRules.encode(), MARC_AUTHORITY, tenantId);
        } else {
          return Future.succeededFuture();
        }
      }).mapEmpty();
  }

  @Override
  public String getFeatureVersion() {
    return "3.9.0";
  }

  @Override
  public String getDescription() {
    return "Authority mapping rules: update rules for name fields with subject metadata";
  }

  private JsonObject updateRules(JsonObject rules) {
    List.of("100", "110", "111", "400", "410", "411", "500", "510", "511")
      .forEach(tag -> updateRulesForTag(tag, rules));
    return rules;
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
