package org.folio.services.converters;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 * Util class contains helper methods for unit testing needs
 */
public final class UnitTestUtil {

  public static String readFileFromPath(String path) throws IOException {
    return FileUtils.readFileToString(new File(path));
  }
}
