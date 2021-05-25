package org.folio.services;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.JobMonitoring;

import java.util.Date;
import java.util.Optional;

/**
 * JobMonitoring Service interface.
 *
 * @see JobMonitoring
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
   * Saves {@link JobMonitoring}
   *
   * @param jobMonitoring {@link JobMonitoring} to save
   * @return future with the id of entity
   */
  Future<String> save(JobMonitoring jobMonitoring, String tenantId);

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
