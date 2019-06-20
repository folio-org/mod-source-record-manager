package org.folio.dao;

import io.vertx.core.Future;
import org.apache.commons.lang3.tuple.Pair;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;

import java.util.List;
import java.util.Optional;

/**
 * DAO interface for the JobExecutionSourceChunk entity
 *
 * @see JobExecutionSourceChunk
 */
public interface JobExecutionSourceChunkDao {

  /**
   * Saves JobExecutionSourceChunk to database
   *
   * @param jobExecutionChunk {@link JobExecutionSourceChunk} to save
   * @return future
   */
  Future<String> save(JobExecutionSourceChunk jobExecutionChunk, String tenantId);

  /**
   * Searches for {@link JobExecutionSourceChunk} in database by {@code query}
   *
   * @param query  query from URL
   * @param offset starting index in a list of results
   * @param limit  limit of records for pagination
   * @return future with list of {@link JobExecutionSourceChunk}
   */
  Future<List<JobExecutionSourceChunk>> get(String query, int offset, int limit, String tenantId);

  /**
   * Searches JobExecutionSourceChunk by id
   *
   * @param id id of the JobExecutionSourceChunk entity
   * @return future with JobExecutionSourceChunk
   */
  Future<Optional<JobExecutionSourceChunk>> getById(String id, String tenantId);

  /**
   * Updates JobExecutionSourceChunk in DB
   *
   * @param jobExecutionChunk entity to update
   * @return future with updated JobExecutionSourceChunk
   */
  Future<JobExecutionSourceChunk> update(JobExecutionSourceChunk jobExecutionChunk, String tenantId);

  /**
   * Deletes JobExecutionSourceChunk from DB
   *
   * @param id id of the {@link JobExecutionSourceChunk} to delete
   * @return future with true if succeeded
   */
  Future<Boolean> delete(String id, String tenantId);

  /**
   * Verifies is all related chunks (to given job execution id) are processed
   * Each chunk is considered being processed if it has ERROR or COMPLETED state.
   *
   * @param jobExecutionId - UUID of related JobExecution
   * @param tenantId       - tenantId
   * @return - returns a pair of boolean values, first indicates whether processing is completed,
   * second - if there is any error
   */
  Future<Pair<Boolean, Boolean>> isAllChunksProcessed(String jobExecutionId, String tenantId);

}
