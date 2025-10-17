package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_BIB;

import java.util.UUID;
import org.folio.services.MappingRuleService;
import org.springframework.stereotype.Component;

@Component
public class BibliographicCancelledLccnMappingRenamingMigration extends CancelledLccnMappingRenamingMigration {

  private static final int ORDER = 5;
  private static final UUID MIGRATION_ID = UUID.fromString("43021382-2631-4a00-9acc-1aaefaf6b202");
  private static final String DESCRIPTION = "Bibliographic mapping rules: rename Cancelled LCCN to Canceled LCCN";

  public BibliographicCancelledLccnMappingRenamingMigration(MappingRuleService mappingRuleService) {
    super(MARC_BIB, ORDER, DESCRIPTION, MIGRATION_ID, mappingRuleService);
  }

}
