package org.folio.services;

import java.util.Optional;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import org.folio.Record;

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
  Future<Optional<JsonObject>> get(Record.RecordType recordType, String tenantId);

  /**
   * Saves default rules from resources classpath
   *
   * @param tenantId tenant
   * @param recordType type of rules (MARC_BIB or MARK_HOLDING)
   * @return future
   */
  Future<Void> saveDefaultRules(Record.RecordType recordType, String tenantId);

  /**
   * Updates rules
   *
   * @param rules    rules
   * @param tenantId tenant
   * @param recordType type of rules (MARC_BIB or MARK_HOLDING)
   * @return rules in JsonObject
   */
  Future<JsonObject> update(String rules, Record.RecordType recordType, String tenantId);

  /**
   * Updates existing rules with default rules
   *
   * @param tenantId tenant
   * @return restored rules
   */
  Future<JsonObject> restore(String tenantId);
}
