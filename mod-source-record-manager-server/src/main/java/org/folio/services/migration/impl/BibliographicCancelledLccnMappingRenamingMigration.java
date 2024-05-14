package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_BIB;

import org.folio.Record;
import org.folio.services.MappingRuleService;
import org.springframework.stereotype.Component;

@Component
public class BibliographicCancelledLccnMappingRenamingMigration extends CancelledLccnMappingRenamingMigration {

  private static final String FEATURE_VERSION = "3.9.0";
  private static final String DESCRIPTION = "Bibliographic mapping rules: rename Cancelled LCCN to Canceled LCCN";

  public BibliographicCancelledLccnMappingRenamingMigration(MappingRuleService mappingRuleService) {
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
    return MARC_BIB;
  }

}
