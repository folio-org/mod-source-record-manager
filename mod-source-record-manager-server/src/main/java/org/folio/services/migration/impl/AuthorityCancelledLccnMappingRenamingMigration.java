package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;

import java.util.UUID;
import org.folio.services.MappingRuleService;
import org.springframework.stereotype.Component;

@Component
public class AuthorityCancelledLccnMappingRenamingMigration extends CancelledLccnMappingRenamingMigration {

  private static final int ORDER = 2;
  private static final UUID MIGRATION_ID = UUID.fromString("85067d3d-5c64-440e-b00b-e952d0c37473");
  private static final String DESCRIPTION = "Authority mapping rules: rename Cancelled LCCN to Canceled LCCN";

  public AuthorityCancelledLccnMappingRenamingMigration(MappingRuleService mappingRuleService) {
    super(MARC_AUTHORITY, ORDER, DESCRIPTION, MIGRATION_ID, mappingRuleService);
  }

  @Override
  public UUID getMigrationId() {
    return MIGRATION_ID;
  }

}
