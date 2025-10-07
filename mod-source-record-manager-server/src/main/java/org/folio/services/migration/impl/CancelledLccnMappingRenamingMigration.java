package org.folio.services.migration.impl;

import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.folio.Record;
import org.folio.services.MappingRuleService;

public abstract class CancelledLccnMappingRenamingMigration extends BaseMappingRulesMigration {

  protected CancelledLccnMappingRenamingMigration(Record.RecordType recordType, int order,
                                                  String description, UUID migrationId,
                                                  MappingRuleService mappingRuleService) {
    super(recordType, order, description, migrationId, mappingRuleService);
  }

  @Override
  protected String updateRules(JsonObject rules) {
    return rules.encode().replace("Cancelled LCCN", "Canceled LCCN");
  }

}
