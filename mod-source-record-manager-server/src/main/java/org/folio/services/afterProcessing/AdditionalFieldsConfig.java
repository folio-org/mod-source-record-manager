package org.folio.services.afterProcessing;

import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

/**
 * Configuration for additional fields
 */
@Component
public class AdditionalFieldsConfig {

  public static final String TAG_999 = "999";
  private static final List<Field> fieldsStorage = Collections.singletonList(
    new Field(
      TAG_999,
      "{\"" + TAG_999 + "\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"i\":\"{instanceId}\"},{\"s\":\"{recordId}\"}]}}"
    )
  );

  public String apply(String tag, Function<String, String> function) {
    for (Field field : fieldsStorage) {
      if (field.getTag().equals(tag)) {
        return function.apply(field.getContent());
      }
    }
    return null;
  }

  private static class Field {
    private String tag;
    private String content;

    public Field(String tag, String content) {
      this.tag = tag;
      this.content = content;
    }

    public String getTag() {
      return tag;
    }

    public String getContent() {
      return content;
    }
  }
}
