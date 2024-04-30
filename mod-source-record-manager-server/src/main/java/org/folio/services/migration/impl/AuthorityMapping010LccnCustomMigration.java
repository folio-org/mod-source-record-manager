package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.services.MappingRuleService;
import org.folio.services.migration.CustomMigration;
import org.springframework.stereotype.Component;

@Component
public class AuthorityMapping010LccnCustomMigration implements CustomMigration {
  private static final String FEATURE_VERSION = "3.9.0";
  private static final String DESCRIPTION = "Authority mapping rules: update rule for LCCN";
  private static final String SUBFIELD_A = "a";
  private static final String SUBFIELD_Z = "z";
  private static final String IDENTIFIER_TYPE_ID = "identifiers.identifierTypeId";
  private static final String DESCRIPTION_TYPE_ID = "Identifier Type for ";
  private static final String IDENTIFIER_VALUE = "identifiers.value";
  private static final String DESCRIPTION_VALUE = "Library of Congress Control Number";
  private static final String CANCELLED = "Cancelled ";
  private static final String LCCN = "LCCN";

  private final MappingRuleService mappingRuleService;

  public AuthorityMapping010LccnCustomMigration(MappingRuleService mappingRuleService) {
    this.mappingRuleService = mappingRuleService;
  }

  @Override
  public Future<Void> migrate(String tenantId) {
    return mappingRuleService.get(MARC_AUTHORITY, tenantId)
      .compose(rules -> {
        if (rules.isPresent()) {
          var newRules = updateRules(rules.get());
          return mappingRuleService.internalUpdate(newRules.encode(), MARC_AUTHORITY, tenantId);
        } else {
          return Future.succeededFuture();
        }
      }).mapEmpty();
  }

  @Override
  public String getDescription() {
    return DESCRIPTION;
  }

  @Override
  public String getFeatureVersion() {
    return FEATURE_VERSION;
  }

  private JsonObject updateRules(JsonObject rules) {
    var tag010Rules = rules.getJsonArray("010");
    if (tag010Rules == null) {
      return rules;
    }
    for (int i = 0; i < tag010Rules.size(); i++) {
      var entities = tag010Rules.getJsonObject(i).getJsonArray("entity");
      if (entities != null) {
        tag010Rules.getJsonObject(i).put("entity", get010FieldEntityJsonArray());
      }
    }
    return rules;
  }

  /**
   * Create entities array of 010 field of Authority mapping rule
   *
   * @return entities JsonArray
   */
  public JsonArray get010FieldEntityJsonArray() {
    return JsonArray.of(
      getEntityJsonObject(IDENTIFIER_TYPE_ID, DESCRIPTION_TYPE_ID + LCCN, SUBFIELD_A),
      getEntityJsonObject(IDENTIFIER_VALUE, DESCRIPTION_VALUE, SUBFIELD_A),
      getEntityJsonObject(IDENTIFIER_TYPE_ID, DESCRIPTION_TYPE_ID + CANCELLED + LCCN, SUBFIELD_Z),
      getEntityJsonObject(IDENTIFIER_VALUE, CANCELLED + DESCRIPTION_VALUE, SUBFIELD_Z)
    );
  }

  private JsonObject getEntityJsonObject(String target, String description, String subfield) {
    return JsonObject.of(
      "target", target,
      "description", description,
      "subfield", JsonArray.of(subfield),
      "rules", getRules(subfield.equals(SUBFIELD_A), target.equals(IDENTIFIER_TYPE_ID))
    );
  }

  private JsonArray getRules(boolean isSubfieldA, boolean isTypeIdRules) {
    String prefix = isSubfieldA ? "" : CANCELLED;
    return JsonArray.of(JsonObject.of("conditions", isTypeIdRules ? getTypeIdConditions(prefix) : getValueConditions()));
  }

  private JsonArray getTypeIdConditions(String prefix) {
    var conditionObject = JsonObject.of(
      "type", "set_identifier_type_id_by_name",
      "parameter", JsonObject.of("name", prefix + LCCN)
    );
    return JsonArray.of(conditionObject);
  }

  private JsonArray getValueConditions() {
    var type = JsonObject.of("type", "trim");
    return JsonArray.of(type);
  }
}
