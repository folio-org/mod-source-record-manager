package org.folio.util;

import io.vertx.core.Vertx;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.SharedData;

public class SharedDataUtil {
  private static String DEFAULT_LOCAL_MAP_NAME = "sharedLocalMap";
  private static String TEST_SHARED_LOCAL_MAP_KEY = "isTesting";

  public static void setIsTesting(Vertx vertx) {
    final SharedData sd = vertx.sharedData();
    final LocalMap<String, String> sharedData = sd.getLocalMap(DEFAULT_LOCAL_MAP_NAME);
    sharedData.put(TEST_SHARED_LOCAL_MAP_KEY, "true");
  }

  public static boolean getIsTesting(Vertx vertx) {
    final SharedData sd = vertx.sharedData();
    final LocalMap<String, Object> sharedData = sd.getLocalMap(DEFAULT_LOCAL_MAP_NAME);
    return Boolean.parseBoolean((String)sharedData.get(TEST_SHARED_LOCAL_MAP_KEY));
  }
}
