package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;

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

  private static final String CANCELLED_STR_VALUE = "Cancelled ";
  private static final String LCCN_STR_VALUE = "LCCN";
  private static final String DESCRIPTION = "description";
  private static final String SUBFIELD = "subfield";

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
    return "Authority mapping rules: update rule for LCCN";
  }

  private JsonObject updateRules(JsonObject rules) {
    var tag010Rules = rules.getJsonArray("010");
    if (tag010Rules == null) {
      return rules;
    }
    for (int i = 0; i < tag010Rules.size(); i++) {
      var entities = tag010Rules.getJsonObject(i).getJsonArray("entity");
      if (entities != null) {
        JsonObject copyIdentifierTypeIdTarget = null;
        JsonObject copyValueTarget = null;
        for (int j = 0; j < entities.size(); j++) {
          var rule = entities.getJsonObject(j);
          if ("identifiers.identifierTypeId".equals(rule.getString("target"))) {
            copyIdentifierTypeIdTarget = rule.copy();

            rule.put(SUBFIELD, JsonArray.of("a"));

            String replacement = rule.getString(DESCRIPTION).replace(LCCN_STR_VALUE, CANCELLED_STR_VALUE + LCCN_STR_VALUE);
            copyIdentifierTypeIdTarget.put(DESCRIPTION, replacement);
            updateRuleCopy(copyIdentifierTypeIdTarget);
          }
          if ("identifiers.value".equals(rule.getString("target"))) {
            copyValueTarget = rule.copy();

            rule.put(SUBFIELD, JsonArray.of("a"));

            String description = rule.getString(DESCRIPTION);
            copyValueTarget.put(DESCRIPTION, CANCELLED_STR_VALUE + description);
            copyValueTarget.put(SUBFIELD, JsonArray.of("z"));
          }
        }
        entities.add(copyIdentifierTypeIdTarget);
        entities.add(copyValueTarget);
      }
    }
    return rules;
  }

  private void updateRuleCopy(JsonObject ruleCopy) {
    ruleCopy.put(SUBFIELD, JsonArray.of("z"));
    var entityRules = ruleCopy.getJsonArray("rules");
    if (entityRules != null) {
      var conditions = entityRules.getJsonObject(0).getJsonArray("conditions");
      if (conditions != null) {
        var parameter = conditions.getJsonObject(0).getJsonObject("parameter");
        if (parameter != null) {
          parameter.put("name", CANCELLED_STR_VALUE + LCCN_STR_VALUE);
        }
      }
    }
  }
}
