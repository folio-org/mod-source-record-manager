package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.folio.services.MappingRuleService;
import org.springframework.stereotype.Component;

@Component
public class AuthorityMappingGeographicSubdivisionCustomMigration extends BaseMappingRulesMigration {

  private static final JsonArray SUBFIELDS = JsonArray.of("v", "x", "y", "z");
  private static final String TARGET_181 = "geographicSubdivision";
  private static final String TARGET_481 = "sftGeographicSubdivision";
  private static final String TARGET_581 = "saftGeographicSubdivision";
  private static final String DESCRIPTION_181 = "Heading geographic subdivision";
  private static final String DESCRIPTION_481 = "See from tracing geographic subdivision";
  private static final String DESCRIPTION_581 = "See also from tracing geographic subdivision";
  private static final String TAG_181 = "181";
  private static final String TAG_481 = "481";
  private static final String TAG_581 = "581";
  private static final int ORDER = 10;
  private static final UUID MIGRATION_ID = UUID.fromString("6ba3ce92-f327-45f3-855f-5d8147885566");
  private static final String DESCRIPTION = "Authority mapping rules: add rules for geographic subdivision fields";

  protected AuthorityMappingGeographicSubdivisionCustomMigration(MappingRuleService mappingRuleService) {
    super(MARC_AUTHORITY, ORDER, DESCRIPTION, MIGRATION_ID, mappingRuleService);
  }

  @Override
  protected String updateRules(JsonObject rules) {
    var field181 = createField(TARGET_181, DESCRIPTION_181, SUBFIELDS, EMPTY_RULES);
    var field481 = createField(TARGET_481, DESCRIPTION_481, SUBFIELDS, EMPTY_RULES);
    var field581 = createField(TARGET_581, DESCRIPTION_581, SUBFIELDS, EMPTY_RULES);
    addFieldIfNotExists(rules, TAG_181, field181);
    addFieldIfNotExists(rules, TAG_481, field481);
    addFieldIfNotExists(rules, TAG_581, field581);
    return sortRules(rules).encode();
  }
}

