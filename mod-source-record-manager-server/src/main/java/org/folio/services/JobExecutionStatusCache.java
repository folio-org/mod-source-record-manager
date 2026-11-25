package org.folio.services;

import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.verticle.consumers.util.JobExecutionUtils;
import org.springframework.stereotype.Component;

/**
 * Injectable access to the process-wide cache of job execution statuses that the chunk consumers consult
 * (see {@link JobExecutionUtils#isNeedToSkip}) to skip chunks of failed or cancelled jobs.
 */
@Component
public class JobExecutionStatusCache {

  public void markAsError(String jobExecutionId) {
    JobExecutionUtils.cache.put(jobExecutionId, JobExecution.Status.ERROR);
  }
}
