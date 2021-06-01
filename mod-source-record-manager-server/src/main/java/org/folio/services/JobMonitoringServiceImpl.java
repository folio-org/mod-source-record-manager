package org.folio.services;

import io.vertx.core.Future;
import java.util.List;
import org.folio.dao.JobMonitoringDao;
import org.folio.rest.jaxrs.model.JobMonitoring;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Optional;
import java.util.UUID;

/**
 * Implementation of the JobMonitoringService.
 *
 * @sse JobMonitoring
 * @see JobMonitoringDao
 * @see JobMonitoringService
 */
@Service
public class JobMonitoringServiceImpl implements JobMonitoringService {
  @Autowired
  private JobMonitoringDao jobMonitoringDao;

  @Override
  public Future<Optional<JobMonitoring>> getByJobExecutionId(String jobExecutionId, String tenantId) {
    return jobMonitoringDao.getByJobExecutionId(jobExecutionId, tenantId);
  }

  @Override
  public Future<List<JobMonitoring>> getAll(String tenantId) {
    return jobMonitoringDao.findAll(tenantId);
  }

  @Override
  public Future<JobMonitoring> saveNew(String jobExecutionId, String tenantId) {
    JobMonitoring jobMonitoring = new JobMonitoring();
    jobMonitoring.setId(UUID.randomUUID().toString());
    jobMonitoring.setJobExecutionId(jobExecutionId);
    jobMonitoring.setLastEventTimestamp(new Date());
    jobMonitoring.setNotificationSent(false);
    return jobMonitoringDao.save(jobMonitoring, tenantId)
      .map(jobMonitoring);
  }

  @Override
  public Future<Boolean> updateByJobExecutionId(String jobExecutionId, Date lastEventTimestamp, boolean notificationSent, String tenantId) {
    return jobMonitoringDao.updateByJobExecutionId(jobExecutionId, lastEventTimestamp, notificationSent, tenantId);
  }

  @Override
  public Future<Boolean> deleteByJobExecutionId(String jobExecutionId, String tenantId) {
    return jobMonitoringDao.deleteByJobExecutionId(jobExecutionId, tenantId);
  }
}
