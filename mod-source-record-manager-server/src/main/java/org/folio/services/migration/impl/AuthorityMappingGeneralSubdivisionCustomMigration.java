package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.Record;
import org.folio.services.MappingRuleService;

public class AuthorityMappingGeneralSubdivisionCustomMigration extends BaseMappingRulesMigration {

  private static final JsonArray SUBFIELDS = JsonArray.of("v", "x", "y", "z");
  private static final String TARGET_180 = "generalSubdivision";
  private static final String TARGET_480 = "sftGeneralSubdivision";
  private static final String TARGET_580 = "saftGeneralSubdivision";
  private static final String DESCRIPTION_180 = "Heading general subdivision";
  private static final String DESCRIPTION_480 = "See from tracing general subdivision";
  private static final String DESCRIPTION_580 = "See also from tracing general subdivision";
  private static final String TAG_180 = "180";
  private static final String TAG_480 = "480";
  private static final String TAG_580 = "580";
  private static final String FEATURE_VERSION = "3.10.0";
  private static final String DESCRIPTION = "Authority mapping rules: add rules for general subdivision fields";

  protected AuthorityMappingGeneralSubdivisionCustomMigration(MappingRuleService mappingRuleService) {
    super(mappingRuleService);
  }

  @Override
  protected Record.RecordType getRecordType() {
    return MARC_AUTHORITY;
  }

  @Override
  protected String updateRules(JsonObject rules) {
    var field180 = createField(TARGET_180, DESCRIPTION_180, SUBFIELDS, EMPTY_RULES);
    var field480 = createField(TARGET_480, DESCRIPTION_480, SUBFIELDS, EMPTY_RULES);
    var field580 = createField(TARGET_580, DESCRIPTION_580, SUBFIELDS, EMPTY_RULES);
    addFieldIfNotExists(rules, TAG_180, field180);
    addFieldIfNotExists(rules, TAG_480, field480);
    addFieldIfNotExists(rules, TAG_580, field580);
    return sortRules(rules).encode();
  }

  @Override
  public String getFeatureVersion() {
    return FEATURE_VERSION;
  }

  @Override
  public String getDescription() {
    return DESCRIPTION;
  }
}
