package org.folio.services.migration.impl;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import java.util.UUID;
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
  private final int order;
  private final String description;
  private final UUID migrationId;

  protected final MappingRuleService mappingRuleService;

  protected BaseMappingRulesMigration(Record.RecordType recordType, int order, String description,
                                      UUID migrationId, MappingRuleService mappingRuleService) {
    this.recordType = recordType;
    this.order = order;
    this.description = description;
    this.migrationId = migrationId;
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
  public int getOrder() {
    return order;
  }

  @Override
  public Record.RecordType getRecordType() {
    return recordType;
  }

  @Override
  public UUID getMigrationId() {
    return migrationId;
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
