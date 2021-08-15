package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import org.folio.Record;

import java.util.Optional;

/**
 * Mapping rules service
 */
public interface MappingRuleService {

  /**
   * Returns rules in JsonObject
   *
   * @param tenantId tenant
   * @param recordType type of rules (MARC_BIB or MARK_HOLDING)
   * @return optional with rules in JsonObject
   */
  Future<Optional<JsonObject>> get(String tenantId, Record.RecordType recordType);

  /**
   * Saves default rules from resources classpath
   *
   * @param tenantId tenant
   * @return future
   */
  Future<Void> saveDefaultRules(String tenantId);

  /**
   * Updates rules
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
