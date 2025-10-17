package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.folio.services.MappingRuleService;
import org.springframework.stereotype.Component;

@Component
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
  private static final int ORDER = 9;
  private static final UUID MIGRATION_ID = UUID.fromString("4f07c706-a740-4e8b-a6a3-1c53f46a314c");
  private static final String DESCRIPTION = "Authority mapping rules: add rules for general subdivision fields";

  protected AuthorityMappingGeneralSubdivisionCustomMigration(MappingRuleService mappingRuleService) {
    super(MARC_AUTHORITY, ORDER, DESCRIPTION, MIGRATION_ID, mappingRuleService);
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
}
