package org.folio.services.migration.impl;

import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.folio.Record;
import org.folio.services.MappingRuleService;
import org.springframework.stereotype.Component;

@Component
public class BibliographicCumulativeIndexFindingAidsCustomMigration extends BaseMappingRulesMigration{

  private static final String DESCRIPTION = "MARC Bib mapping rules: "
                                            + "update 'Cumulative Index / Finding Aids notes' rule";
  private static final UUID MIGRATION_ID = UUID.fromString("4ff91f43-6007-4d97-b36a-fbe6e9dd4d06");
  private static final int ORDER = 15;

  protected BibliographicCumulativeIndexFindingAidsCustomMigration(MappingRuleService mappingRuleService) {
    super(Record.RecordType.MARC_BIB, ORDER, DESCRIPTION, MIGRATION_ID, mappingRuleService);
  }

  @Override
  protected String updateRules(JsonObject rules) {
    return rules.encode().replace("Cumulative Index / Finding Aides notes", "Cumulative Index / Finding Aids notes");
  }
}
