package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;
import static org.folio.Record.RecordType.MARC_HOLDING;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.services.MappingRuleService;
import org.folio.services.migration.CustomMigration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AuthorityMapping010LccnCustomMigration implements CustomMigration {

  @Autowired
  private MappingRuleService mappingRuleService;

  @Override
  public Future<Void> migrate(String tenantId) {
    return mappingRuleService.get(MARC_AUTHORITY, tenantId)
      .compose(rules -> {
        if (rules.isPresent()) {
          var newRules = updateRules(rules.get());
          return mappingRuleService.update(newRules.encode(), MARC_AUTHORITY, tenantId);
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
    return "Authority mapping rules: update rule for LCCN";
  }

  private JsonObject updateRules(JsonObject rules) {
    var tag010Rules = rules.getJsonArray("010");
    if (tag010Rules == null) {
      return rules;
    }
    // todo Implement insertion
    for (int i = 0; i < tag010Rules.size(); i++) {
      var entities = tag010Rules.getJsonObject(i).getJsonArray("entity");
      if (entities != null) {
        for (int j = 0; j < entities.size(); j++) {
          var rule = entities.getJsonObject(j);
          if ("identifiers.identifierTypeId".equals(rule.getString("target"))) {
            rule.put("subfield", JsonArray.of("z"));
          }
        }
      }
    }
    return rules;
  }
}
