package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import org.folio.Record;

import java.util.Optional;

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
  Future<Optional<JsonObject>> get(String tenantId, Record.RecordType ... recordType);

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
   * @param rules    rules
   * @param tenantId tenant
   * @return updated rules
   */
  Future<JsonObject> update(JsonObject rules, String tenantId);
}
