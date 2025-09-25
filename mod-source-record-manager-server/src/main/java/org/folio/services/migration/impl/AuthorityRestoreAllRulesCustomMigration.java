package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;

import io.vertx.core.Future;
import java.util.UUID;
import org.folio.Record;
import org.folio.services.MappingRuleService;
import org.folio.services.migration.CustomMigration;
import org.springframework.stereotype.Component;

@Component
public class AuthorityRestoreAllRulesCustomMigration implements CustomMigration {

  private static final int ORDER = 13;
  private static final UUID MIGRATION_ID = UUID.fromString("5f798252-d4b2-453d-8261-147fd5a3ec83");
  private static final String DESCRIPTION = "Authority mapping rules: restore all rules to default";

  private final  MappingRuleService mappingRuleService;

  public AuthorityRestoreAllRulesCustomMigration(MappingRuleService mappingRuleService) {
    this.mappingRuleService = mappingRuleService;
  }

  @Override
  public int getOrder() {
    return ORDER;
  }

  @Override
  public Record.RecordType getRecordType() {
    return MARC_AUTHORITY;
  }

  @Override
  public String getDescription() {
    return DESCRIPTION;
  }

  @Override
  public Future<Void> migrate(String tenantId) {
    return mappingRuleService.restore(Record.RecordType.MARC_AUTHORITY, tenantId)
      .compose(rules -> Future.succeededFuture())
      .mapEmpty();
  }

  @Override
  public UUID getMigrationId() {
    return MIGRATION_ID;
  }
}
