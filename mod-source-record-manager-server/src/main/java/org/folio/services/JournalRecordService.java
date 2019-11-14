package org.folio.services;

import io.vertx.core.Future;

/**
 * JournalRecord Service interface.
 */
public interface JournalRecordService {

  /**
   * Deletes journal records associated with job execution by specified id
   *
   * @param jobExecutionId job execution id
   * @param tenantId tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> deleteByJobExecutionId(String jobExecutionId, String tenantId);
}
