package org.folio.services;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

@Component
public class MappingRulesCache {

  private static final Logger LOG = LoggerFactory.getLogger(MappingRulesCache.class);

  private MappingRuleService mappingRuleService;
  private AsyncLoadingCache<String, Optional<JsonObject>> cache;

  @Autowired
  public MappingRulesCache(MappingRuleService mappingRuleService, Vertx vertx) {
    this.mappingRuleService = mappingRuleService;
    cache = Caffeine.newBuilder()
      .executor(task -> vertx.runOnContext(ar -> task.run()))
      .maximumSize(1)
      .buildAsync((key, executor) -> {
        CompletableFuture<Optional<JsonObject>> future = new CompletableFuture<>();
        executor.execute(() -> mappingRuleService.get(key)
          .onFailure(throwable -> future.completeExceptionally(throwable))
          .onSuccess(optional -> future.complete(optional)));
        return future;
      });
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
