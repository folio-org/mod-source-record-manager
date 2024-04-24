package org.folio.services.migration.helper;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FieldMappingHelper {
  private static final String SUBFIELD_A = "a";
  private static final String SUBFIELD_Z = "z";
  private static final String IDENTIFIER_TYPE_ID = "identifiers.identifierTypeId";
  private static final String DESCRIPTION_TYPE_ID = "Identifier Type for ";
  private static final String IDENTIFIER_VALUE = "identifiers.value";
  private static final String DESCRIPTION_VALUE = "Library of Congress Control Number";
  private static final String CANCELLED = "Cancelled ";
  private static final String LCCN = "LCCN";


  private FieldMappingHelper() {
    throw new IllegalStateException("Utility class");
  }

  public static JsonArray get010FieldEntityJsonArray() {
    return new JsonArray(List.of(
      getEntityJsonObject(IDENTIFIER_TYPE_ID, DESCRIPTION_TYPE_ID + LCCN, SUBFIELD_A),
      getEntityJsonObject(IDENTIFIER_VALUE, DESCRIPTION_VALUE, SUBFIELD_A),
      getEntityJsonObject(IDENTIFIER_TYPE_ID, DESCRIPTION_TYPE_ID + CANCELLED + LCCN, SUBFIELD_Z),
      getEntityJsonObject(IDENTIFIER_VALUE, CANCELLED + DESCRIPTION_VALUE, SUBFIELD_Z)
    ));
  }

  private static JsonObject getEntityJsonObject(String target, String description, String subfield) {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("target", target);
    map.put("description", description);
    map.put("subfield", new JsonArray(List.of(subfield)));
    map.put("rules", getRules(subfield.equals(SUBFIELD_A), target.equals(IDENTIFIER_TYPE_ID)));

    return new JsonObject(map);
  }

  private static JsonArray getRules(boolean isSubfieldA, boolean isTypeIdRules) {
    String prefix = isSubfieldA ? "" : CANCELLED;
    return new JsonArray(
      List.of(Map.of("conditions", isTypeIdRules ? getTypeIdConditions(prefix) : getValueConditions()))
    );
  }

  private static JsonArray getTypeIdConditions(String prefix) {
    var name = new JsonObject(Map.of("name", prefix + LCCN));
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("type", "set_identifier_type_id_by_name");
    map.put("parameter", name);

    return new JsonArray(List.of(new JsonObject(map)));
  }

  private static JsonArray getValueConditions() {
    var type = new JsonObject(Map.of("type", "trim"));

    return new JsonArray(List.of(type));
  }
}
