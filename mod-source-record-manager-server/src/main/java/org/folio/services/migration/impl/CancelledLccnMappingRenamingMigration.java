package org.folio.services.migration.impl;

import io.vertx.core.Future;
import org.folio.Record.RecordType;
import org.folio.services.MappingRuleService;
import org.folio.services.migration.CustomMigration;

public abstract class CancelledLccnMappingRenamingMigration implements CustomMigration {

  private static final String FEATURE_VERSION = "3.9.0";
  private static final String DESCRIPTION = "Authority mapping rules: rename Cancelled LCCN to Canceled LCCN";

  private final MappingRuleService mappingRuleService;

  public CancelledLccnMappingRenamingMigration(MappingRuleService mappingRuleService) {
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
