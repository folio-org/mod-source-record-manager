package org.folio.services.progress;

import org.apache.commons.lang3.StringUtils;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.JobExecutionProgress;

/**
 * Class that enforces the presence of tenant ID and Job execution ID. These values must be present for appropriate batch
 * processing.
 */
public class BatchableJobExecutionProgress {
  private final JobExecutionProgress jobExecutionProgress;
  private final OkapiConnectionParams params;

  public BatchableJobExecutionProgress(OkapiConnectionParams params, JobExecutionProgress jobExecutionProgress) {
    if (params == null || StringUtils.isBlank(params.getTenantId())) {
      throw new IllegalArgumentException("Tenant ID must be set in Okapi connection parameters");
    }
    if (jobExecutionProgress == null || StringUtils.isBlank(jobExecutionProgress.getJobExecutionId())) {
      throw new IllegalArgumentException("job execution id must be set on JobExecutionProgress object");
    }
    this.params = params;
    this.jobExecutionProgress = jobExecutionProgress;
  }

  public JobExecutionProgress getJobExecutionProgress() {
    return jobExecutionProgress;
  }

  public OkapiConnectionParams getParams() {
    return params;
  }
}
