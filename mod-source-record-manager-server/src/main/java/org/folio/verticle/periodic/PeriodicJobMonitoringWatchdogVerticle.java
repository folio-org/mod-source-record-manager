package org.folio.verticle.periodic;

import java.util.Date;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import javax.ws.rs.NotFoundException;

import io.vertx.core.shareddata.LocalMap;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;

import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobMonitoring;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.RunBy;
import org.folio.services.JobExecutionService;
import org.folio.services.JobMonitoringService;

@Log4j2
@Component
@PropertySource("classpath:application.properties")
public class PeriodicJobMonitoringWatchdogVerticle extends AbstractPeriodicJobVerticle {

  @Value("${job.monitoring.inactive.interval.max.ms}")
  private long maxInactiveInterval;

  @Autowired
  private JobMonitoringService jobMonitoringService;
  @Autowired
  private JobExecutionService jobExecutionService;

  @Override
  protected long getExecutionIntervalInMs() {
    return maxInactiveInterval;
  }

  @Override
  protected void executePeriodicJob() {
    getTenants().forEach(tenantId -> {
      log.debug("Check tenant [{}] for stacked jobs", tenantId);
      jobMonitoringService.getInactiveJobMonitors(maxInactiveInterval, tenantId)
        .onSuccess(jobMonitors ->
          jobMonitors.forEach(jobMonitoring -> sendNotificationForInactiveJob(tenantId, jobMonitoring))
        );
    });
  }

  private Set<String> getTenants() {
    LocalMap<String, Integer> tenants = vertx.sharedData().getLocalMap("tenants");
    return tenants.keySet();
  }

  private void sendNotificationForInactiveJob(String tenantId, JobMonitoring jobMonitoring) {
    String jobExecutionId = jobMonitoring.getJobExecutionId();
    jobExecutionService.getJobExecutionById(jobExecutionId, tenantId)
      .map(getJobExecutionOrFail(jobExecutionId))
      .onSuccess(jobExecution -> printNotificationWarnLog(jobExecution, tenantId))
      .onSuccess(jobExecution -> changeNotificationFlagToTrue(jobMonitoring, tenantId));
  }

  private Function<Optional<JobExecution>, JobExecution> getJobExecutionOrFail(String jobExecutionId) {
    return optJob -> optJob.orElseThrow(() -> new NotFoundException(
      String.format("Couldn't find JobExecution with id %s", jobExecutionId)));
  }

  private void printNotificationWarnLog(JobExecution jobExecution, String tenantId) {
    log.warn("Data Import Job with jobExecutionId = {} not progressing for tenant = {}, "
        + "current time = {}, run by = {}, file name = {}, job profile info = {}, start date = {}, stop date = {}",
      jobExecution.getId(), tenantId, new Date(), getRunBy(jobExecution), jobExecution.getFileName(),
      getJobProfileInfo(jobExecution), jobExecution.getStartedDate(), jobExecution.getCompletedDate()
    );
  }

  private void changeNotificationFlagToTrue(JobMonitoring jobMonitoring, String tenantId) {
    String jobExecutionId = jobMonitoring.getJobExecutionId();
    Date lastEventTimestamp = jobMonitoring.getLastEventTimestamp();
    jobMonitoringService.updateByJobExecutionId(jobExecutionId, lastEventTimestamp, true, tenantId);
  }

  private String getRunBy(JobExecution jobExecution) {
    RunBy runBy = jobExecution.getRunBy();
    return runBy != null ? runBy.getFirstName() + " " + runBy.getLastName() : null;
  }

  private String getJobProfileInfo(JobExecution jobExecution) {
    JobProfileInfo jobProfileInfo = jobExecution.getJobProfileInfo();
    return jobProfileInfo != null ? String.format("jobProfileInfoId: '%s', name: '%s', dataType: '%s'",
      jobProfileInfo.getId(), jobProfileInfo.getName(), jobProfileInfo.getDataType()) : null;
  }
}
