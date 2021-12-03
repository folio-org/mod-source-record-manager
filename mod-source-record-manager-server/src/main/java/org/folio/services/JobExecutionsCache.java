package org.folio.services;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.JobExecutionFilter;
import org.folio.rest.jaxrs.model.JobExecutionDtoCollection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * In-memory cache for the job executions and DI landing page
 */
@Component
public class JobExecutionsCache {

  private static final Logger LOGGER = LogManager.getLogger();

  private Integer expireInSeconds;
  private JobExecutionService jobExecutionService;
  // Pair of tenant id and conditions from JobExecutionFilter
  private AsyncLoadingCache<Pair<String, String>, Optional<JobExecutionDtoCollection>> cache;
  private Vertx vertx;

  @Autowired
  public JobExecutionsCache(JobExecutionService jobExecutionService, Vertx vertx,
                            @Value("${srm.job.execution.cache.expire.seconds:30}") Integer expireInSeconds) {
    this.expireInSeconds = expireInSeconds;
    this.jobExecutionService = jobExecutionService;
    this.vertx = vertx;
    cache = buildCache();
  }

  public Future<JobExecutionDtoCollection> get(String tenantId, JobExecutionFilter filter, String sortBy, String order, int offset, int limit) {
    Promise<JobExecutionDtoCollection> promise = Promise.promise();
    String uniqueQuery = appendSortAndPagingParams(filter, sortBy, order, offset, limit);
    cache.get(Pair.of(tenantId, uniqueQuery)).whenComplete((jobExecutionOptional, e) -> {
      if (e == null) {
        if (jobExecutionOptional.isPresent()) {
          promise.complete(jobExecutionOptional.get());
        } else {
          jobExecutionService.getJobExecutionsWithoutParentMultiple(filter, sortBy, order, offset, limit, tenantId)
            .onSuccess(ar -> {
              put(tenantId, uniqueQuery, ar);
              promise.complete(ar);
            })
            .onFailure(cause -> {
              LOGGER.error("Failure to get job executions without parent", cause);
              promise.fail(cause);
            });
        }
      } else {
        promise.fail(e);
      }
    });
    return promise.future();
  }


  private String appendSortAndPagingParams(JobExecutionFilter filter, String sortBy, String order, int offset, int limit) {
    return String.format("%s sortBy=%s/%s LIMIT %d OFFSET %d", filter.buildWhereClause(), sortBy, order, limit, offset);
  }

  private void put(String tenantId, String cqlQuery, JobExecutionDtoCollection jobExecutionCollection) {
    cache.put(Pair.of(tenantId, cqlQuery), CompletableFuture.completedFuture(Optional.of(jobExecutionCollection)));
  }

  private AsyncLoadingCache<Pair<String, String>, Optional<JobExecutionDtoCollection>> buildCache() {
    return Caffeine.newBuilder()
      .executor(task -> vertx.runOnContext(ar -> task.run()))
      .expireAfterWrite(expireInSeconds, TimeUnit.SECONDS)
      .buildAsync((key, executor) -> CompletableFuture.completedFuture(Optional.empty()));
  }

  public void evictCache() {
    cache = buildCache();
  }
}
