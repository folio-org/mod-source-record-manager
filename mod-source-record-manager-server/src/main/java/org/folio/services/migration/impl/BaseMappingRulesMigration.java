package org.folio.services.migration.impl;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import org.folio.Record;
import org.folio.services.MappingRuleService;
import org.folio.services.migration.CustomMigration;

public abstract class BaseMappingRulesMigration implements CustomMigration {

  protected final MappingRuleService mappingRuleService;

  protected BaseMappingRulesMigration(MappingRuleService mappingRuleService) {
    this.mappingRuleService = mappingRuleService;
  }

  @Override
  public Future<Void> migrate(String tenantId) {
    return mappingRuleService.get(getRecordType(), tenantId)
      .compose(rules -> {
        if (rules.isPresent()) {
          var newRules = updateRules(rules.get());
          return mappingRuleService.internalUpdate(newRules, getRecordType(), tenantId);
        } else {
          return Future.succeededFuture();
        }
      }).mapEmpty();
  }

  protected void addFieldIfNotExists(JsonObject rules, String tag, JsonObject field) {
    if (!rules.containsKey(tag)) {
      rules.put(tag, JsonArray.of(field));
    }
  }

  protected JsonObject createField(String target, String description, JsonArray subfields, JsonArray rules) {
    return new JsonObject()
      .put("target", target)
      .put("description", description)
      .put("subfield", subfields)
      .put("rules", rules);
  }

  protected JsonObject sortRules(JsonObject rules) {
    return rules.stream()
      .sorted(Map.Entry.comparingByKey())
      .collect(JsonObject::new, (json, entry) -> json.put(entry.getKey(), entry.getValue()), JsonObject::mergeIn);
  }

  protected abstract Record.RecordType getRecordType();

  protected abstract String updateRules(JsonObject rules);
}
