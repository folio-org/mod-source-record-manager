package org.folio.verticle.periodic;

import lombok.extern.log4j.Log4j2;
import org.folio.dao.JobExecutionDao;
import org.folio.services.TenantDataProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@PropertySource("classpath:application.properties")
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

  private long timerId;

  @Override
  public void startPeriodicJob() {
    timerId = vertx.setPeriodic(permanentDeletionInterval, handler -> proceedForJobExecutionPermanentDeletion());
  }

  @Override
  public void stop() throws Exception {
    vertx.cancelTimer(timerId);
    super.stop();
  }

  private void proceedForJobExecutionPermanentDeletion() {
    tenantDataProvider.getModuleTenants()
      .onSuccess(allTenants -> allTenants.forEach(tenantName -> {
        log.info("Check tenant [{}] for marked as deleted jobs", tenantName);
        jobExecutionDao.hardDeleteJobExecutions(tenantName, diffNumberOfDays)
          .onSuccess(rows -> log.info("Permanent Job Executions Deletion completed for the tenant {}", tenantName))
          .onFailure(throwable -> log.error("Permanent Job Executions Deletion did not complete for the tenant {}", tenantName, throwable));
        }))
      .onFailure(throwable -> log.error("Tenants Not Found For Permanent Job Executions Deletion", throwable));
  }
}
