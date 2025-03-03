package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.services.MappingRuleService;
import org.springframework.stereotype.Component;

@Component
public class AuthorityMappingNamedEventCustomMigration extends BaseMappingRulesMigration {

  private static final JsonArray SUBFIELDS = JsonArray.of("a", "c", "d", "g", "v", "x", "y", "z");
  private static final String TARGET_147 = "namedEvent";
  private static final String TARGET_447 = "sftNamedEvent";
  private static final String TARGET_547 = "saftNamedEvent";
  private static final String DESCRIPTION_147 = "Heading named event";
  private static final String DESCRIPTION_447 = "See from tracing named event";
  private static final String DESCRIPTION_547 = "See also from tracing named event";
  private static final String TAG_147 = "147";
  private static final String TAG_447 = "447";
  private static final String TAG_547 = "547";
  private static final String FEATURE_VERSION = "3.10.0";
  private static final String DESCRIPTION = "Authority mapping rules: add rules for named event fields";

  protected AuthorityMappingNamedEventCustomMigration(MappingRuleService mappingRuleService) {
    super(MARC_AUTHORITY, FEATURE_VERSION, DESCRIPTION, mappingRuleService);
  }

  @Override
  protected String updateRules(JsonObject rules) {
    var field147 = createField(TARGET_147, DESCRIPTION_147, SUBFIELDS, EMPTY_RULES);
    var field447 = createField(TARGET_447, DESCRIPTION_447, SUBFIELDS, EMPTY_RULES);
    var field547 = createField(TARGET_547, DESCRIPTION_547, SUBFIELDS, EMPTY_RULES);
    addFieldIfNotExists(rules, TAG_147, field147);
    addFieldIfNotExists(rules, TAG_447, field447);
    addFieldIfNotExists(rules, TAG_547, field547);
    return sortRules(rules).encode();
  }
}
