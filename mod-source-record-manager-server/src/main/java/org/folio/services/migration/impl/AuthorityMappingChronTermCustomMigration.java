package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.folio.services.MappingRuleService;
import org.springframework.stereotype.Component;

@Component
public class AuthorityMappingChronTermCustomMigration extends BaseMappingRulesMigration{

  private static final JsonArray SUBFIELDS = JsonArray.of("a", "v", "x", "y", "z");
  private static final String TARGET_148 = "chronTerm";
  private static final String TARGET_448 = "sftChronTerm";
  private static final String TARGET_548 = "saftChronTerm";
  private static final String DESCRIPTION_148 = "Heading chronological term";
  private static final String DESCRIPTION_448 = "See from tracing chronological term";
  private static final String DESCRIPTION_548 = "See also from tracing chronological term";
  private static final String TAG_148 = "148";
  private static final String TAG_448 = "448";
  private static final String TAG_548 = "548";
  private static final int ORDER = 7;
  private static final UUID MIGRATION_ID = UUID.fromString("f27072af-e322-4574-8667-81c066c41ac8");
  private static final String DESCRIPTION = "Authority mapping rules: add rules for chronological term fields";

  protected AuthorityMappingChronTermCustomMigration(MappingRuleService mappingRuleService) {
    super(MARC_AUTHORITY, ORDER, DESCRIPTION, MIGRATION_ID, mappingRuleService);
  }

  @Override
  protected String updateRules(JsonObject rules) {
    var field148 = createField(TARGET_148, DESCRIPTION_148, SUBFIELDS, EMPTY_RULES);
    var field448 = createField(TARGET_448, DESCRIPTION_448, SUBFIELDS, EMPTY_RULES);
    var field548 = createField(TARGET_548, DESCRIPTION_548, SUBFIELDS, EMPTY_RULES);
    addFieldIfNotExists(rules, TAG_148, field148);
    addFieldIfNotExists(rules, TAG_448, field448);
    addFieldIfNotExists(rules, TAG_548, field548);
    return sortRules(rules).encode();
  }
}

