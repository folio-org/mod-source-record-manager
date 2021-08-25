package org.folio.dao;

import io.vertx.core.Future;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;

import java.util.Optional;

/**
 * Mapping parameters snapshot DAO
 */
public interface MappingParamsSnapshotDao {
  /**
   * Returns mapping parameters 3for given JobExecution
   *
   * @param jobExecutionId JobExecution id
   * @param tenantId       tenant id
   * @return optional of mapping parameters snapshot
   */
  Future<Optional<MappingParameters>> getByJobExecutionId(String jobExecutionId, String tenantId);

  /**
   * Saves mapping parameters snapshot
   *
   * @param params         mapping parameters snapshot
   * @param jobExecutionId JobExecution id
   * @param tenantId       tenant id
   * @return id
   */
  Future<String> save(MappingParameters params, String jobExecutionId, String tenantId);

  /**
   * Deletes mapping parameters snapshot
   *
   * @param jobExecutionId JobExecution id
   * @param tenantId       tenant id
   * @return true if deleted successfully
   */
  Future<Boolean> delete(String jobExecutionId, String tenantId);
}
