package org.folio.verticle.periodic;

import lombok.extern.log4j.Log4j2;
import org.folio.dao.JobExecutionDao;
import org.folio.services.TenantDataProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class PeriodicDeleteJobExecutionVerticle extends AbstractPeriodicJobVerticle {
  /**
   * Default unit time for job execution permanent delete interval = 24 hours.
   */
  @Value("${periodic.job.execution.permanent.delete.interval.ms:86400000}")
  private long permanentDeletionInterval;
  /**
   * Default unit time for comparing marked as deleted jobs available to hard delete = 2 days.
   */
  @Value("${job.execution.difference.number.of.days:2}")
  private long diffNumberOfDays;

  @Autowired
  private JobExecutionDao jobExecutionDao;
  @Autowired
  private TenantDataProvider tenantDataProvider;

  @Override
  protected long getExecutionIntervalInMs() {
    return permanentDeletionInterval;
  }

  @Override
  protected void executePeriodicJob() {
    tenantDataProvider.getModuleTenants()
      .onSuccess(allTenants -> allTenants.forEach(tenantId -> {
        log.info("Check tenant [{}] for marked as deleted jobs", tenantId);
        jobExecutionDao.hardDeleteJobExecutions(diffNumberOfDays, tenantId)
          .onSuccess(rows -> log.info("executePeriodicJob:: Permanent Job Executions Deletion completed for the tenant {}", tenantId))
          .onFailure(throwable -> log.warn("executePeriodicJob:: Permanent Job Executions Deletion did not complete for the tenant {}", tenantId, throwable));
      }))
      .onFailure(throwable -> log.warn("executePeriodicJob:: Tenants Not Found For Permanent Job Executions Deletion", throwable));
  }
}
