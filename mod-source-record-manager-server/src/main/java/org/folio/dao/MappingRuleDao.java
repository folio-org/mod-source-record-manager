package org.folio.dao;

import java.util.Optional;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import org.folio.Record;

/**
 * Mapping rules DAO
 */
public interface MappingRuleDao {
  /**
   * Returns default rules represented in JsonObject for given tenant
   *
   * @param tenantId tenant
   * @param recordType type of rules (MARC_BIB or MARK_HOLDING)
   * @return optional of rules
   */
  Future<Optional<JsonObject>> get(Record.RecordType recordType, String tenantId);

  /**
   * Saves rules
   *
   * @param rules    rules
   * @param tenantId tenant
   * @return rules id
   */
  Future<String> save(JsonObject rules, Record.RecordType recordType, String tenantId);

  /**
   * Updates rules if exist
   *
   * @param rules    rules
   * @param tenantId tenant
   * @return updated rules
   */
  Future<JsonObject> update(JsonObject rules, String tenantId);
}
