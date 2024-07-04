package org.folio.services.migration.impl;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
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

  protected abstract Record.RecordType getRecordType();

  protected abstract String updateRules(JsonObject rules);
}
