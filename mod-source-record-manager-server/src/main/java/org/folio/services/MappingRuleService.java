package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import java.util.Optional;

/**
 * Mapping rules service
 */
public interface MappingRuleService {

  /**
   * Returns rules in JsonObject
   *
   * @param tenantId tenant
   * @return optional with rules in JsonObject
   */
  Future<Optional<JsonObject>> get(String tenantId);

  /**
   * Saves default rules from resources classpath
   *
   * @param tenantId tenant
   * @return future
   */
  Future<Void> saveDefaultRules(String tenantId);

  /**
   * Updates default rules
   *
   * @param rules    rules
   * @param tenantId tenant
   * @return rules in JsonObject
   */
  Future<JsonObject> update(String rules, String tenantId);

  /**
   * Updates existing rules with default rules
   *
   * @param tenantId tenant
   * @return restored rules
   */
  Future<JsonObject> restore(String tenantId);
}
