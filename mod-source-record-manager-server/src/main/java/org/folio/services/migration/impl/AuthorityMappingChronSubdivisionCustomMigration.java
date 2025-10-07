package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.folio.services.MappingRuleService;
import org.springframework.stereotype.Component;

@Component
public class AuthorityMappingChronSubdivisionCustomMigration extends BaseMappingRulesMigration {

  private static final JsonArray SUBFIELDS = JsonArray.of("v", "x", "y", "z");
  private static final String TARGET_182 = "chronSubdivision";
  private static final String TARGET_482 = "sftChronSubdivision";
  private static final String TARGET_582 = "saftChronSubdivision";
  private static final String DESCRIPTION_182 = "Heading chronological subdivision";
  private static final String DESCRIPTION_482 = "See from tracing chronological subdivision";
  private static final String DESCRIPTION_582 = "See also from tracing chronological subdivision";
  private static final String TAG_182 = "182";
  private static final String TAG_482 = "482";
  private static final String TAG_582 = "582";
  private static final int ORDER = 6;
  private static final UUID MIGRATION_ID = UUID.fromString("9e1d0eb6-4965-4e56-af56-45d0677c0ddc");
  private static final String DESCRIPTION = "Authority mapping rules: add rules for chronological subdivision fields";

  protected AuthorityMappingChronSubdivisionCustomMigration(MappingRuleService mappingRuleService) {
    super(MARC_AUTHORITY, ORDER, DESCRIPTION, MIGRATION_ID, mappingRuleService);
  }

  @Override
  protected String updateRules(JsonObject rules) {
    var field182 = createField(TARGET_182, DESCRIPTION_182, SUBFIELDS, EMPTY_RULES);
    var field482 = createField(TARGET_482, DESCRIPTION_482, SUBFIELDS, EMPTY_RULES);
    var field582 = createField(TARGET_582, DESCRIPTION_582, SUBFIELDS, EMPTY_RULES);
    addFieldIfNotExists(rules, TAG_182, field182);
    addFieldIfNotExists(rules, TAG_482, field482);
    addFieldIfNotExists(rules, TAG_582, field582);
    return sortRules(rules).encode();
  }
}
