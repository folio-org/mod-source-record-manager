package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import java.util.Optional;

public interface MappingRuleDao {
  /**
   *
   * @return
   */
  Future<Optional<JsonObject>> get(String tenantId);

  /**
   *
   * @param rule
   * @param tenantId
   * @return
   */
  Future<String> save(JsonObject rule, String tenantId);

  /**
   *
   * @param rule
   * @return
   */
  Future<JsonObject> update(JsonObject rule, String tenantId);
}
