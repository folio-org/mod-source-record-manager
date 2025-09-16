package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_BIB;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.services.MappingRuleService;
import org.folio.services.migration.CustomMigration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class MarcBibMapping338FormatCustomMigration implements CustomMigration {

  @Autowired
  private MappingRuleService mappingRuleService;

  @Override
  public Future<Void> migrate(String tenantId) {
    return mappingRuleService.get(MARC_BIB, tenantId)
      .compose(rules -> {
        if (rules.isPresent()) {
          var newRules = updateRules(rules.get());
          return mappingRuleService.update(newRules.encode(), MARC_BIB, tenantId);
        } else {
          return Future.succeededFuture();
        }
      }).mapEmpty();
  }

  @Override
  public String getFeatureVersion() {
    return "4.0.0";
  }

  @Override
  public String getDescription() {
    return "MARC Bib mapping rules: update rule for formatId in 338 field";
  }

  private JsonObject updateRules(JsonObject rules) {
    var tag338Rules = rules.getJsonArray("338");
    if (tag338Rules == null) {
      return rules;
    }
    for (int i = 0; i < tag338Rules.size(); i++) {
      var rule = tag338Rules.getJsonObject(i);
          if ("instanceFormatIds".equals(rule.getString("target"))) {
            rule.put("applyRulesOnConcatenatedData", Boolean.TRUE);
            rule.put("subfield", JsonArray.of("a", "b"));
            rule.put("subFieldDelimiter", JsonArray.of(new JsonObject()
              .put("value", "~")
              .put("subfields", JsonArray.of("a", "b"))));
          }
      }
    return rules;
  }
}
