package org.folio.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.shareddata.LocalMap;
import java.util.Date;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobMonitoring;
import org.folio.services.JobExecutionService;
import org.folio.services.JobMonitoringService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.stereotype.Component;

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

  @Override
  public void start(Promise<Void> startPromise) {
    declareSpringContext();
    String tenantId = getTenantId();

    vertx.setPeriodic(watchdogTimestamp, handler -> {
      jobMonitoringService.getAll(tenantId).onSuccess(jobMonitors ->
        jobMonitors.forEach(jobMonitoring -> {
          long lastEventTimestamp = getDifferenceTimestamp(jobMonitoring.getLastEventTimestamp());
          if (lastEventTimestamp >= watchdogTimestamp) {
            findJobExecutionById(tenantId, jobMonitoring);
          }
        })
      );
    });
  }

  protected void declareSpringContext() {
    context.put("springContext", springGlobalContext);
    SpringContextUtil.autowireDependencies(this, context);
  }

  private long getDifferenceTimestamp(Date lastEventTimestamp) {
    return new Date().toInstant().toEpochMilli() - lastEventTimestamp.toInstant().toEpochMilli();
  }

  private void findJobExecutionById(String tenantId, JobMonitoring jobMonitoring) {
    jobExecutionService.getJobExecutionById(jobMonitoring.getJobExecutionId(), tenantId)
      .onSuccess(optionalJobExecution -> optionalJobExecution.ifPresent(this::printWarnLog));
  }

  private void printWarnLog(JobExecution jobExecution) {
    LOGGER.warn("Data Import Job with jobExecutionId = {} not progressing, current time = {}, run by = {}, "
        + "file name = {}, job profile info = {}, start date = {}, stop date = {}",
      jobExecution.getId(), new Date(), printRunBy(jobExecution), jobExecution.getFileName(),
      printJobProfile(jobExecution), jobExecution.getStartedDate(), jobExecution.getCompletedDate()
    );
  }

  private Object printJobProfile(JobExecution jobExecution) {
    return jobExecution.getJobProfileInfo() != null ? String
      .format("jobProfileInfoId: '%s', name: '%s', dataType: '%s'", jobExecution.getJobProfileInfo().getId(),
        jobExecution.getJobProfileInfo().getName(), jobExecution.getJobProfileInfo().getDataType()) : null;
  }

  private Object printRunBy(JobExecution jobExecution) {
    return jobExecution.getRunBy() != null ? jobExecution.getRunBy().getFirstName() + " " + jobExecution.getRunBy()
      .getLastName() : null;
  }

  private String getTenantId() {
    LocalMap<String, String> tenants = vertx.sharedData().getLocalMap("tenants");
    String tenantId = tenants.get("tenant");
    LOGGER.info("Tenant id: {} ", tenantId);
    return tenantId;
  }

  public static void setSpringContext(AbstractApplicationContext springContext) {
    JobMonitoringWatchdogVerticle.springGlobalContext = springContext;
  }

}
