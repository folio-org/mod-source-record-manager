package org.folio.verticle.consumers.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.folio.rest.jaxrs.model.JobExecution;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class JobExecutionUtils {
  public static final List<JobExecution.Status> SKIP_STATUSES = Arrays.asList(JobExecution.Status.CANCELLED);
  public static boolean isNeedToSkip(JobExecution jobExecution) {
    return Objects.nonNull(jobExecution.getStatus()) && SKIP_STATUSES.contains(jobExecution.getStatus());
  }

}
