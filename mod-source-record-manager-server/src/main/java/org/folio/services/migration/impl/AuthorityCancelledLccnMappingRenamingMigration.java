package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;

import org.folio.Record;
import org.folio.services.MappingRuleService;
import org.springframework.stereotype.Component;

@Component
public class AuthorityCancelledLccnMappingRenamingMigration extends CancelledLccnMappingRenamingMigration {

  private static final String FEATURE_VERSION = "3.9.0";
  private static final String DESCRIPTION = "Authority mapping rules: rename Cancelled LCCN to Canceled LCCN";

  public AuthorityCancelledLccnMappingRenamingMigration(MappingRuleService mappingRuleService) {
    super(mappingRuleService);
  }

  @Override
  public String getFeatureVersion() {
    return FEATURE_VERSION;
  }

  @Override
  public String getDescription() {
    return DESCRIPTION;
  }

  @Override
  protected Record.RecordType getRecordType() {
    return MARC_AUTHORITY;
  }

}
