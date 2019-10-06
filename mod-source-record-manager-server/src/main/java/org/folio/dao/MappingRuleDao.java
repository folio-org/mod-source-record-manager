package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import java.util.Optional;

/**
 * Mapping rules DAO
 */
public interface MappingRuleDao {
  /**
   * Returns default rules represented in JsonObject for given tenant
   *
   * @param tenantId tenant
   * @return optional of rules
   */
  Future<Optional<JsonObject>> get(String tenantId);

  /**
   * Saves rules
   *
   * @param rules    rules
   * @param tenantId tenant
   * @return rules id
   */
  Future<String> save(JsonObject rules, String tenantId);

  /**
   * Updates rules if exist
   *
   * @param rules rules
   * @param tenantId tenant
   * @return updated rules
   */
  Future<JsonObject> update(JsonObject rules, String tenantId);
}
