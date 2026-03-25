package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import java.util.UUID;
import org.folio.services.MappingRuleService;
import org.springframework.stereotype.Component;

@Component
public class AuthorityMappingIdentifierTypeCodeCustomMigration extends BaseMappingRulesMigration {

  private static final int ORDER = 16;
  private static final UUID MIGRATION_ID = UUID.fromString("b3e5a4c1-7f2d-4e8a-9b6c-1d0f3a5e7c9b");
  private static final String DESCRIPTION =
    "Authority mapping rules: switch identifier type resolution from name-based to code-based";

  private static final String OLD_TYPE_BY_NAME = "set_identifier_type_id_by_name";
  private static final String OLD_TYPE_BY_VALUE = "set_identifier_type_id_by_value";
  private static final String NEW_TYPE = "set_authority_identifier_type_id_by_code";
  private static final String CONDITIONS = "conditions";
  private static final String TYPE = "type";
  private static final String PARAMETER = "parameter";
  private static final String NAME = "name";
  private static final String NAMES = "names";
  private static final String CODE = "code";
  private static final String RULES = "rules";
  private static final String ENTITY = "entity";

  private static final Map<String, String> NAME_TO_CODE = Map.of(
    "Control number", "control-number",
    "LCCN", "lccn",
    "Canceled LCCN", "canceled-lccn",
    "Other standard identifier", "other-standard-identifier",
    "System control number", "system-control-number"
  );

  protected AuthorityMappingIdentifierTypeCodeCustomMigration(MappingRuleService mappingRuleService) {
    super(MARC_AUTHORITY, ORDER, DESCRIPTION, MIGRATION_ID, mappingRuleService);
  }

  @Override
  protected String updateRules(JsonObject rules) {
    for (String tag : new String[] {"001", "010", "024", "035"}) {
      var tagRules = rules.getJsonArray(tag);
      if (tagRules != null) {
        updateTagRules(tagRules);
      }
    }
    return rules.encode();
  }

  private void updateTagRules(JsonArray tagRules) {
    for (int i = 0; i < tagRules.size(); i++) {
      var field = tagRules.getJsonObject(i);
      updateFieldRules(field);
      var entityArray = field.getJsonArray(ENTITY);
      if (entityArray != null) {
        for (int j = 0; j < entityArray.size(); j++) {
          updateFieldRules(entityArray.getJsonObject(j));
        }
      }
    }
  }

  private void updateFieldRules(JsonObject field) {
    var rulesArray = field.getJsonArray(RULES);
    if (rulesArray == null) {
      return;
    }
    for (int i = 0; i < rulesArray.size(); i++) {
      var rule = rulesArray.getJsonObject(i);
      var conditions = rule.getJsonArray(CONDITIONS);
      if (conditions != null) {
        updateConditions(conditions);
      }
    }
  }

  private void updateConditions(JsonArray conditions) {
    for (int i = 0; i < conditions.size(); i++) {
      var condition = conditions.getJsonObject(i);
      var type = condition.getString(TYPE);
      if (OLD_TYPE_BY_NAME.equals(type)) {
        updateNameCondition(condition);
      } else if (OLD_TYPE_BY_VALUE.equals(type)) {
        updateValueCondition(condition);
      }
    }
  }

  private void updateNameCondition(JsonObject condition) {
    var parameter = condition.getJsonObject(PARAMETER);
    if (parameter == null) {
      return;
    }
    var name = parameter.getString(NAME);
    var code = NAME_TO_CODE.get(name);
    if (code != null) {
      condition.put(TYPE, NEW_TYPE);
      condition.put(PARAMETER, new JsonObject().put(CODE, code));
    }
  }

  private void updateValueCondition(JsonObject condition) {
    var parameter = condition.getJsonObject(PARAMETER);
    if (parameter == null) {
      return;
    }
    var names = parameter.getJsonArray(NAMES);
    if (names != null && !names.isEmpty()) {
      var firstName = names.getString(0);
      var code = NAME_TO_CODE.get(firstName);
      if (code != null) {
        condition.put(TYPE, NEW_TYPE);
        condition.put(PARAMETER, new JsonObject().put(CODE, code));
      }
    }
  }
}
