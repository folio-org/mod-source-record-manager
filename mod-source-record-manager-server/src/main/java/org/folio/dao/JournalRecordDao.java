package org.folio.dao;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.JournalRecord;

import java.util.List;

/**
 * DAO interface for the JournalRecord entity
 */
public interface JournalRecordDao {

  /**
   * Saves JournalRecord entity to DB
   *
   * @param journalRecord journalRecord entity to save
   * @param tenantId tenant id
   * @return future with created journalRecord id
   */
  Future<String> save(JournalRecord journalRecord, String tenantId);

  /**
   * Searches for JournalRecord entities by jobExecutionId
   *
   * @param jobExecutionId job execution id
   * @param tenantId tenant id
   * @return future with list of journalRecord entities
   */
  Future<List<JournalRecord>> getByJobExecutionId(String jobExecutionId, String tenantId);

  /**
   * Deletes journal records associated with job execution by specified jobExecutionId
   *
   * @param jobExecutionId job execution id
   * @param tenantId tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> deleteByJobExecutionId(String jobExecutionId, String tenantId);
}
