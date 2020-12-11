package org.folio.services;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dao.MappingRuleDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

@Component
public class MappingRuleCache {

  private static final Logger LOG = LoggerFactory.getLogger(MappingRuleCache.class);

  private MappingRuleDao mappingRuleDao;
  private AsyncLoadingCache<String, Optional<JsonObject>> cache;

  @Autowired
  public MappingRuleCache(MappingRuleDao mappingRuleDao, Vertx vertx) {
    this.mappingRuleDao = mappingRuleDao;
    cache = Caffeine.newBuilder()
      .executor(task -> vertx.runOnContext(ar -> task.run()))
      .buildAsync((key, executor) -> loadMappingRules(key, executor, mappingRuleDao));
  }

  private CompletableFuture<Optional<JsonObject>> loadMappingRules(String tenantId, Executor executor, MappingRuleDao mappingRuleDao) {
    CompletableFuture<Optional<JsonObject>> future = new CompletableFuture<>();
    executor.execute(() -> mappingRuleDao.get(tenantId).onComplete(ar -> {
      if (ar.failed()) {
        LOG.error("Failed to load mapping rules for tenant '{}' from data base", ar.cause(), tenantId);
        future.completeExceptionally(ar.cause());
        return;
      }
      future.complete(ar.result());
    }));
    return future;
  }

  public Future<Optional<JsonObject>> get(String tenantId) {
    Promise<Optional<JsonObject>> promise = Promise.promise();
    cache.get(tenantId).whenComplete((rulesOptional, e) -> {
      if (e == null) {
        promise.complete(rulesOptional);
      } else {
        promise.fail(e);
      }
    });
    return promise.future();
  }

  public void put(String tenantId, JsonObject mappingRules) {
    cache.put(tenantId, CompletableFuture.completedFuture(Optional.of(mappingRules)));
  }
}
