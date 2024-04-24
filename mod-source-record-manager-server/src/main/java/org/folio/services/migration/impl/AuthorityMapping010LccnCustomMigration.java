package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import org.folio.services.MappingRuleService;
import org.folio.services.migration.CustomMigration;
import org.folio.services.migration.helper.FieldMappingHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AuthorityMapping010LccnCustomMigration implements CustomMigration {
  private static final String FEATURE_VERSION = "3.9.0";
  private static final String DESCRIPTION = "Authority mapping rules: update rule for LCCN";

  @Autowired
  private MappingRuleService mappingRuleService;

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
    return FEATURE_VERSION;
  }

  @Override
  public String getDescription() {
    return DESCRIPTION;
  }

  private JsonObject updateRules(JsonObject rules) {
    var tag010Rules = rules.getJsonArray("010");
    if (tag010Rules == null) {
      return rules;
    }
    for (int i = 0; i < tag010Rules.size(); i++) {
      var entities = tag010Rules.getJsonObject(i).getJsonArray("entity");
      if (entities != null) {
        tag010Rules.getJsonObject(i).put("entity", FieldMappingHelper.get010FieldEntityJsonArray());
      }
    }
    return rules;
  }
}
