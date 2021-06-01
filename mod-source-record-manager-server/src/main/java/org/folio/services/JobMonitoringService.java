package org.folio.services;

import io.vertx.core.Future;
import java.util.List;
import org.folio.dao.JobMonitoringDao;
import org.folio.rest.jaxrs.model.JobMonitoring;

import java.util.Date;
import java.util.Optional;

/**
 * JobMonitoring Service interface.
 *
 * @see JobMonitoring
 * @see JobMonitoringDao
 */
public interface JobMonitoringService {
  /**
   * Searches for {@link JobMonitoring} by jobExecutionId
   *
   * @param jobExecutionId jobExecution id
   * @return future with Optional of JobMonitoring.
   * Returns succeeded future with an empty Optional if the entity does not exist by the given
   */
  Future<Optional<JobMonitoring>> getByJobExecutionId(String jobExecutionId, String tenantId);

  /**
   * Searches all {@link JobMonitoring}
   *
   * @return future with Optional of JobMonitoring.
   * Returns succeeded future with list of entities
   */
  Future<List<JobMonitoring>> getAll(String tenantId);

  /**
   * Creates and saves the new {@link JobMonitoring} entity
   *
   * @param jobExecutionId  job execution id
   * @return future with the entity
   */
  Future<JobMonitoring> saveNew(String jobExecutionId, String tenantId);

  /**
   * Updates {@link JobMonitoring} by the given jobExecutionId
   *
   * @param jobExecutionId     job execution id
   * @param lastEventTimestamp the timestamp when the event occurred
   * @param notificationSent   indicates whether notification has been sent of not
   * @param tenantId           tenant id
   * @return future with true if the entity has been successfully updated
   */
  Future<Boolean> updateByJobExecutionId(String jobExecutionId, Date lastEventTimestamp, boolean notificationSent, String tenantId);

  /**
   * Deletes {@link JobMonitoring} by jobExecutionId
   *
   * @param jobExecutionId jobExecution id
   * @param tenantId       tenant id
   * @return future with true if the entity has been successfully deleted
   */
  Future<Boolean> deleteByJobExecutionId(String jobExecutionId, String tenantId);
}
