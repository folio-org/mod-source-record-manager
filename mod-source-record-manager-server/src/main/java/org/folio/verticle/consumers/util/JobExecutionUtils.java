package org.folio.verticle.consumers.util;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.folio.rest.jaxrs.model.JobExecution;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class JobExecutionUtils {

  public static final List<JobExecution.Status> SKIP_STATUSES = Arrays.asList(JobExecution.Status.CANCELLED, JobExecution.Status.ERROR);

  private static final Executor cacheExecutor = JobExecutionUtils::execute;
  public static final Cache<String, JobExecution.Status> cache = Caffeine.newBuilder().maximumSize(20).executor(cacheExecutor).build();

  public static boolean isNeedToSkip(JobExecution jobExecution) {
    JobExecution.Status prevJobExecStat = null;
    if (jobExecution.getId() != null) {
      prevJobExecStat = cache.getIfPresent(jobExecution.getId());
    }
    return Objects.nonNull(jobExecution.getStatus()) && SKIP_STATUSES.contains(
      prevJobExecStat != null ? prevJobExecStat :
        jobExecution.getStatus());
  }

  public static void clearCache() {
    cache.invalidateAll();
  }

  private static void execute(Runnable serviceExecutor) {
    Context context = Vertx.currentContext();
    if (context != null) {
      context.runOnContext(ar -> serviceExecutor.run());
    } else {
      // The common pool below is used because it is the  default executor for caffeine
      ForkJoinPool.commonPool().execute(serviceExecutor);
    }
  }
}
