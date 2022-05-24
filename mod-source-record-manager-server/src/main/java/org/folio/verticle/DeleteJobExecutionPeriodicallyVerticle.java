package org.folio.verticle;

import io.vertx.core.AbstractVerticle;
import lombok.extern.log4j.Log4j2;
import org.folio.dao.JobExecutionDao;
import org.folio.dao.util.TenantUtil;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.stereotype.Component;

@Log4j2
@Component
@PropertySource("classpath:application.properties")
public class DeleteJobExecutionPeriodicallyVerticle extends AbstractVerticle {

  private static AbstractApplicationContext springGlobalContext;

  @Autowired
  private JobExecutionDao jobExecutionDao;

  @Value("${job.execution.permanent.delete.interval.ms:86400000}")
  private long jobExecutionPermanentDeletionInterval;
  private long timerId;

  public static void setSpringContext(AbstractApplicationContext springContext) {
    DeleteJobExecutionPeriodicallyVerticle.springGlobalContext = springContext;
  }

  @Override
  public void start() {
    declareSpringContext();

    timerId = vertx.setPeriodic(jobExecutionPermanentDeletionInterval, handler -> proceedForJobExecutionPermanentDeletion());
  }

  @Override
  public void stop() throws Exception {
    vertx.cancelTimer(timerId);
    super.stop();
  }

  protected void declareSpringContext() {
    context.put("springContext", springGlobalContext);
    SpringContextUtil.autowireDependencies(this, context);
  }

  private void proceedForJobExecutionPermanentDeletion() {
    TenantUtil.getModuleTenants(vertx)
        .onSuccess(allTenants -> allTenants.forEach(tenantName -> {
          log.debug("Check tenant [{}] for stacked jobs", tenantName);
          if(jobExecutionDao.permanentDeleteJobExecutions(tenantName)){
            log.info("Permanent Job Execution Deletion completed for the tenant {}", tenantName);
          }else{
            log.warn("Permanent Job Execution Deletion did not complete successfully for the tenant {}", tenantName);
          }
        }))
      .onFailure(throwable -> log.error("Tenants Not Found For Permanent Job Execution Deletion : ", throwable));
  }

}
