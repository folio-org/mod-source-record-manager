package org.folio.verticle;

import java.util.Date;
import java.util.Set;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.shareddata.LocalMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.stereotype.Component;

import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobMonitoring;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.RunBy;
import org.folio.services.JobExecutionService;
import org.folio.services.JobMonitoringService;
import org.folio.spring.SpringContextUtil;

@Component
@PropertySource("classpath:application.properties")
public class JobMonitoringWatchdogVerticle extends AbstractVerticle {

  private static final Logger LOGGER = LogManager.getLogger();
  private static AbstractApplicationContext springGlobalContext;

  @Autowired
  private JobMonitoringService jobMonitoringService;

  @Autowired
  private JobExecutionService jobExecutionService;

  @Value("${job.monitoring.watchdog.timestamp}")
  private Long watchdogTimestamp;

  public static void setSpringContext(AbstractApplicationContext springContext) {
    JobMonitoringWatchdogVerticle.springGlobalContext = springContext;
  }

  @Override
  public void start(Promise<Void> startPromise) {
    declareSpringContext();

    vertx.setPeriodic(watchdogTimestamp, handler -> getTenants()
      .forEach(tenantId -> {
        LOGGER.info("Check tenant [{}] for stacked jobs", tenantId);
        jobMonitoringService.getAll(tenantId).onSuccess(jobMonitors ->
          jobMonitors.forEach(jobMonitoring -> {
            long lastEventTimestamp = getDifferenceTimestamp(jobMonitoring.getLastEventTimestamp());
            if (lastEventTimestamp >= watchdogTimestamp) {
              findJobExecutionById(tenantId, jobMonitoring);
            }
          })
        );
      })
    );
    startPromise.complete();
  }

  protected void declareSpringContext() {
    context.put("springContext", springGlobalContext);
    SpringContextUtil.autowireDependencies(this, context);
  }

  private Set<String> getTenants() {
    LocalMap<String, Integer> tenants = vertx.sharedData().getLocalMap("tenants");
    return tenants.keySet();
  }

  private long getDifferenceTimestamp(Date lastEventTimestamp) {
    return new Date().toInstant().toEpochMilli() - lastEventTimestamp.toInstant().toEpochMilli();
  }

  private void findJobExecutionById(String tenantId, JobMonitoring jobMonitoring) {
    jobExecutionService.getJobExecutionById(jobMonitoring.getJobExecutionId(), tenantId)
      .onSuccess(optionalJobExec -> optionalJobExec.ifPresent(jobExecution -> printWarnLog(jobExecution, tenantId)));
  }

  private void printWarnLog(JobExecution jobExecution, String tenantId) {
    LOGGER.warn("Data Import Job with jobExecutionId = {} not progressing for tenant = {}, "
        + "current time = {}, run by = {}, file name = {}, job profile info = {}, start date = {}, stop date = {}",
      jobExecution.getId(), tenantId, new Date(), printRunBy(jobExecution), jobExecution.getFileName(),
      printJobProfile(jobExecution), jobExecution.getStartedDate(), jobExecution.getCompletedDate()
    );
  }

  private String printRunBy(JobExecution jobExecution) {
    RunBy runBy = jobExecution.getRunBy();
    return runBy != null ? runBy.getFirstName() + " " + runBy.getLastName() : null;
  }

  private Object printJobProfile(JobExecution jobExecution) {
    JobProfileInfo jobProfileInfo = jobExecution.getJobProfileInfo();
    return jobProfileInfo != null ? String.format("jobProfileInfoId: '%s', name: '%s', dataType: '%s'",
      jobProfileInfo.getId(), jobProfileInfo.getName(), jobProfileInfo.getDataType()) : null;
  }
}
