package org.folio.services.migration.impl;

import io.vertx.core.Future;
import org.folio.Record.RecordType;
import org.folio.services.MappingRuleService;
import org.folio.services.migration.CustomMigration;

public abstract class CancelledLccnMappingRenamingMigration implements CustomMigration {

  private final MappingRuleService mappingRuleService;

  protected CancelledLccnMappingRenamingMigration(MappingRuleService mappingRuleService) {
    this.mappingRuleService = mappingRuleService;
  }

  @Override
  public Future<Void> migrate(String tenantId) {
    return mappingRuleService.get(getRecordType(), tenantId)
      .compose(rules -> {
        if (rules.isPresent()) {
          var newRules = rules.get().encode().replace("Cancelled LCCN", "Canceled LCCN");
          return mappingRuleService.internalUpdate(newRules, getRecordType(), tenantId);
        } else {
          return Future.succeededFuture();
        }
      }).mapEmpty();
  }

  protected abstract RecordType getRecordType();

}
