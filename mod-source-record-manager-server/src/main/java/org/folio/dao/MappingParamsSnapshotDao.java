package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import java.util.Optional;

/**
 * Mapping parameters snapshot DAO
 */
public interface MappingParamsSnapshotDao {
  /**
   * Returns mapping parameters represented in JsonObject for given JobExecution
   *
   * @param jobExecutionId JobExecution id
   * @param tenantId       tenant id
   * @return optional of mapping parameters snapshot
   */
  Future<Optional<JsonObject>> getByJobExecutionId(String jobExecutionId, String tenantId);

  /**
   * Saves mapping parameters snapshot
   *
   * @param params          mapping parameters snapshot
   * @param jobExecutionId JobExecution id
   * @param tenantId       tenant id
   * @return rules id
   */
  Future<String> save(JsonObject params, String jobExecutionId, String tenantId);

  /**
   * Deletes mapping parameters snapshot
   *
   * @param jobExecutionId JobExecution id
   * @param tenantId tenant id
   * @return true if deleted successfully
   */
  Future<Boolean> delete(String jobExecutionId, String tenantId);
}
