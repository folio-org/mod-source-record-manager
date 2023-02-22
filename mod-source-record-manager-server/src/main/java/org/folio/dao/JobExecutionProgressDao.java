package org.folio.dao;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionProgress;
import org.folio.rest.persist.Conn;
import org.folio.rest.persist.SQLConnection;

import java.util.Optional;
import java.util.function.UnaryOperator;

/**
 * DAO interface for the JobExecutionProgress entity
 */
public interface JobExecutionProgressDao {

  /**
   * Searches for jobExecutionProgress by {@link JobExecution} id
   *
   * @param jobExecutionId jobExecution id
   * @param tenantId       tenant id
   * @return future with jobExecutionProgress
   */
  Future<Optional<JobExecutionProgress>> getByJobExecutionId(String jobExecutionId, String tenantId);

  /**
   * Creates jobExecutionProgress for {@link JobExecution} with specified jobExecutionId
   *
   * @param connection     transaction connection
   * @param jobExecutionId jobExecution id
   * @param totalRecords   total number of records to be processed
   * @param tenantId       tenant id
   * @return future with created JobExecutionProgress
   */
  Future<JobExecutionProgress> initializeJobExecutionProgress(AsyncResult<SQLConnection> connection, String jobExecutionId, Integer totalRecords, String tenantId);

  /**
   * Saves jobExecutionProgress entity to database
   *
   * @param progress entity to save
   * @param tenantId tenant id
   * @return future
   */
  Future<RowSet<Row>> save(JobExecutionProgress progress, String tenantId);

  /**
   * Updates jobExecutionProgress entity by jobExecutionId in database
   *
   * @param jobExecutionId  jobExecution id
   * @param progressMutator defines changes on entity for update
   * @param tenantId        tenant id
   * @return future with updated jobExecutionProgress
   */
  Future<JobExecutionProgress> updateByJobExecutionId(String jobExecutionId, UnaryOperator<JobExecutionProgress> progressMutator, String tenantId);

  /**
   * Updates jobExecutionProgress entity by jobExecutionId in database by adding delta to existing success and error
   * counts.
   *
   * @param jobExecutionId  jobExecution id
   * @param successCountDelta number of successful executions
   * @param errorCountDelta number of failed executions
   * @param tenantId        tenant id
   * @return future with updated jobExecutionProgress
   */
  Future<JobExecutionProgress> updateCompletionCounts(String jobExecutionId, int successCountDelta, int errorCountDelta, String tenantId);
}
