package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.folio.services.MappingRuleService;
import org.springframework.stereotype.Component;

@Component
public class AuthorityMappingMediumPerfTermCustomMigration extends BaseMappingRulesMigration {

  private static final JsonArray SUBFIELDS = JsonArray.of("a");
  private static final String TARGET_162 = "mediumPerfTerm";
  private static final String TARGET_462 = "sftMediumPerfTerm";
  private static final String TARGET_562 = "saftMediumPerfTerm";
  private static final String DESCRIPTION_162 = "Heading medium of performance term";
  private static final String DESCRIPTION_462 = "See from tracing medium of performance term";
  private static final String DESCRIPTION_562 = "See also from tracing medium of performance term";
  private static final String TAG_162 = "162";
  private static final String TAG_462 = "462";
  private static final String TAG_562 = "562";
  private static final int ORDER = 11;
  private static final UUID MIGRATION_ID = UUID.fromString("fa562314-18b1-4cc1-9585-a06530a25809");
  private static final String DESCRIPTION = "Authority mapping rules: add rules for medium of performance term fields";

  protected AuthorityMappingMediumPerfTermCustomMigration(MappingRuleService mappingRuleService) {
    super(MARC_AUTHORITY, ORDER, DESCRIPTION, MIGRATION_ID, mappingRuleService);
  }

  @Override
  protected String updateRules(JsonObject rules) {
    var field162 = createField(TARGET_162, DESCRIPTION_162, SUBFIELDS, EMPTY_RULES);
    var field462 = createField(TARGET_462, DESCRIPTION_462, SUBFIELDS, EMPTY_RULES);
    var field562 = createField(TARGET_562, DESCRIPTION_562, SUBFIELDS, EMPTY_RULES);
    addFieldIfNotExists(rules, TAG_162, field162);
    addFieldIfNotExists(rules, TAG_462, field462);
    addFieldIfNotExists(rules, TAG_562, field562);
    return sortRules(rules).encode();
  }
}

