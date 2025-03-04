package org.folio.services.migration.impl;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import org.folio.Record;
import org.folio.services.MappingRuleService;
import org.folio.services.migration.CustomMigration;

public abstract class BaseMappingRulesMigration implements CustomMigration {

  private static final String TARGET = "target";
  private static final String FIELD_DESCRIPTION = "description";
  private static final String SUBFIELD = "subfield";
  private static final String RULES = "rules";

  protected static final JsonArray EMPTY_RULES = new JsonArray();

  private final Record.RecordType recordType;
  private final String featureVersion;
  private final String description;

  protected final MappingRuleService mappingRuleService;

  protected BaseMappingRulesMigration(Record.RecordType recordType, String featureVersion, String description,
                                      MappingRuleService mappingRuleService) {
    this.recordType = recordType;
    this.featureVersion = featureVersion;
    this.description = description;
    this.mappingRuleService = mappingRuleService;
  }

  @Override
  public Future<Void> migrate(String tenantId) {
    return mappingRuleService.get(recordType, tenantId)
      .compose(rules -> {
        if (rules.isPresent()) {
          var newRules = updateRules(rules.get());
          return mappingRuleService.internalUpdate(newRules, recordType, tenantId);
        } else {
          return Future.succeededFuture();
        }
      }).mapEmpty();
  }

  @Override
  public String getFeatureVersion() {
    return featureVersion;
  }

  @Override
  public String getDescription() {
    return description;
  }

  protected void addFieldIfNotExists(JsonObject rules, String tag, JsonObject field) {
    if (!rules.containsKey(tag)) {
      rules.put(tag, JsonArray.of(field));
    }
  }

  protected JsonObject createField(String target, String description, JsonArray subfields, JsonArray rules) {
    return new JsonObject()
      .put(TARGET, target)
      .put(FIELD_DESCRIPTION, description)
      .put(SUBFIELD, subfields)
      .put(RULES, rules);
  }

  protected JsonObject sortRules(JsonObject rules) {
    return rules.stream()
      .sorted(Map.Entry.comparingByKey())
      .collect(JsonObject::new, (json, entry) -> json.put(entry.getKey(), entry.getValue()), JsonObject::mergeIn);
  }

  protected abstract String updateRules(JsonObject rules);
}
