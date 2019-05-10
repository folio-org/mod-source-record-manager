package org.folio.services.afterprocessing;

import io.vertx.core.json.JsonObject;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;

/**
 * Configuration for additional fields
 */
@Component
public class AdditionalFieldsConfig {

  public static final String TAG_999 = "999";
  private static final List<Field> STORAGE = Collections.singletonList(
    new Field(TAG_999, new JsonObject("{\"" + TAG_999 + "\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[]}}"))
  );

  public JsonObject getFieldByTag(String tag) {
    for (Field field : STORAGE) {
      if (field.getTag().equals(tag)) {
        return field.getContent();
      }
    }
    return null;
  }

  /**
   * Internal class helper to describe field structure
   */
  private static class Field {
    private String tag;
    private JsonObject content;

    public Field(String tag, JsonObject content) {
      this.tag = tag;
      this.content = content;
    }

    public String getTag() {
      return tag;
    }

    public JsonObject getContent() {
      return content;
    }
  }
}
