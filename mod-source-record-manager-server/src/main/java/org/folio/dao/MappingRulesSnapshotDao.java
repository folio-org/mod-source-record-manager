package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import java.util.Optional;

/**
 * Mapping rules snapshot DAO
 */
public interface MappingRulesSnapshotDao {
  /**
   * Returns mapping rules represented in JsonObject for given JobExecution
   *
   * @param jobExecutionId JobExecution id
   * @param tenantId       tenant id
   * @return optional of rules snapshot
   */
  Future<Optional<JsonObject>> getByJobExecutionId(String jobExecutionId, String tenantId);

  /**
   * Saves rules snapshot
   *
   * @param rules          rules snapshot
   * @param jobExecutionId JobExecution id
   * @param tenantId       tenant id
   * @return rules id
   */
  Future<String> save(JsonObject rules, String jobExecutionId, String tenantId);

  /**
   * Deletes rules snapshot
   *
   * @param jobExecutionId JobExecution id
   * @param tenantId tenant id
   * @return true if deleted successfully
   */
  Future<Boolean> delete(String jobExecutionId, String tenantId);
}
