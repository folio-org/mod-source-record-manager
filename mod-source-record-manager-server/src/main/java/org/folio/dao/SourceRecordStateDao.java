package org.folio.dao;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.SourceRecordState;

import java.util.Optional;

/**
 * Source Record state in QM DAO
 */
public interface SourceRecordStateDao {

  /**
   * Returns SourceRecordState by sourceRecordId for given tenant
   *
   * @param tenantId tenant
   * @return optional of rules
   */
  Future<Optional<SourceRecordState>> get(String sourceRecordId, String tenantId);

  /**
   * Saves SourceRecordState
   *
   * @param state    state
   * @param tenantId tenant
   * @return rules id
   */
  Future<String> save(SourceRecordState state, String tenantId);

  /**
   * Updates state if exist
   *
   * @param state    state
   * @param tenantId tenant
   * @return updated rules
   */
  Future<SourceRecordState> update(SourceRecordState state, String tenantId);
}
