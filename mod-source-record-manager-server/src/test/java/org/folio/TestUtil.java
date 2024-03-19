package org.folio;

import io.vertx.core.json.JsonObject;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

import static org.folio.services.afterprocessing.AdditionalFieldsUtil.TAG_999;

/**
 * Util class contains helper methods for unit testing needs
 */
public final class TestUtil {

  public static String readFileFromPath(String path) throws IOException {
    return new String(FileUtils.readFileToByteArray(new File(path)));
  }

  public static boolean recordsHaveSameOrder(String baseContent, String newContent) {
    var baseJson = new JsonObject(baseContent);
    var newJson = new JsonObject(newContent);

    var baseFields = baseJson.getJsonArray("fields");
    var newFields = newJson.getJsonArray("fields");

    for (Object newFieldObject : newFields) {
      var newField = (JsonObject) newFieldObject;
      if (newField.containsKey(TAG_999)) {
        continue;
      }
      boolean foundMatchingField = false;
      for (Object baseFieldObject : baseFields) {
        var baseField = (JsonObject) baseFieldObject;
        if (baseField.containsKey(TAG_999)) {
          continue;
        }
        if (Objects.equals(baseField, newField)) {
          foundMatchingField = true;
          break;
        }
      }
      if (!foundMatchingField) {
        return false;
      }
    }
    return true;
  }
}
