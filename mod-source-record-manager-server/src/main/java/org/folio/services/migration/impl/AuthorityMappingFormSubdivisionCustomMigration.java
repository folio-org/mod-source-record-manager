package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.services.MappingRuleService;
import org.springframework.stereotype.Component;

@Component
public class AuthorityMappingFormSubdivisionCustomMigration extends BaseMappingRulesMigration {

  private static final JsonArray SUBFIELDS = JsonArray.of("v", "x", "y", "z");
  private static final String TARGET_185 = "formSubdivision";
  private static final String TARGET_485 = "sftFormSubdivision";
  private static final String TARGET_585 = "saftFormSubdivision";
  private static final String DESCRIPTION_185 = "Heading form subdivision";
  private static final String DESCRIPTION_485 = "See from tracing form subdivision";
  private static final String DESCRIPTION_585 = "See also from tracing form subdivision";
  private static final String TAG_185 = "185";
  private static final String TAG_485 = "485";
  private static final String TAG_585 = "585";
  private static final String FEATURE_VERSION = "3.10.0";
  private static final String DESCRIPTION = "Authority mapping rules: add rules for form subdivision fields";

  protected AuthorityMappingFormSubdivisionCustomMigration(MappingRuleService mappingRuleService) {
    super(MARC_AUTHORITY, FEATURE_VERSION, DESCRIPTION, mappingRuleService);
  }

  @Override
  protected String updateRules(JsonObject rules) {
    var field185 = createField(TARGET_185, DESCRIPTION_185, SUBFIELDS, EMPTY_RULES);
    var field485 = createField(TARGET_485, DESCRIPTION_485, SUBFIELDS, EMPTY_RULES);
    var field585 = createField(TARGET_585, DESCRIPTION_585, SUBFIELDS, EMPTY_RULES);
    addFieldIfNotExists(rules, TAG_185, field185);
    addFieldIfNotExists(rules, TAG_485, field485);
    addFieldIfNotExists(rules, TAG_585, field585);
    return sortRules(rules).encode();
  }
}

