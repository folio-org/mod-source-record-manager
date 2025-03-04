package org.folio.services.migration.impl;

import io.vertx.core.json.JsonObject;
import org.folio.Record;
import org.folio.services.MappingRuleService;

public abstract class CancelledLccnMappingRenamingMigration extends BaseMappingRulesMigration {

  protected CancelledLccnMappingRenamingMigration(Record.RecordType recordType, String featureVersion,
                                                  String description, MappingRuleService mappingRuleService) {
    super(recordType, featureVersion, description, mappingRuleService);
  }

  @Override
  protected String updateRules(JsonObject rules) {
    return rules.encode().replace("Cancelled LCCN", "Canceled LCCN");
  }

}
