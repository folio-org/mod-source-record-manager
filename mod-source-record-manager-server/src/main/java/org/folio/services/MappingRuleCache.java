package org.folio.services;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.folio.dao.MappingRuleDao;
import org.folio.services.entity.MappingRuleCacheKey;

/**
 * In-memory cache for the mapping rules
 */
@Component
public class MappingRuleCache {

  private static final Logger LOGGER = LogManager.getLogger();

  private final AsyncLoadingCache<MappingRuleCacheKey, Optional<JsonObject>> cache;

  @Autowired
  public MappingRuleCache(MappingRuleDao mappingRuleDao, Vertx vertx) {
    cache = Caffeine.newBuilder()
      .executor(task -> vertx.runOnContext(ar -> task.run()))
      .buildAsync((key, executor) -> loadMappingRules(key, executor, mappingRuleDao));
  }

  private CompletableFuture<Optional<JsonObject>> loadMappingRules(MappingRuleCacheKey key, Executor executor, MappingRuleDao mappingRuleDao) {
    CompletableFuture<Optional<JsonObject>> future = new CompletableFuture<>();
    executor.execute(() -> mappingRuleDao.get(key.getRecordType(), key.getTenantId()).onComplete(ar -> {
      if (ar.failed()) {
        LOGGER.error("Failed to load mapping rules for tenant '{}' from data base", key.getTenantId(), ar.cause());
        future.completeExceptionally(ar.cause());
        return;
      }
      future.complete(ar.result());
    }));
    return future;
  }

  /**
   * Returns mapping rules associated with specified tenant id
   * @param key contains tenantId and recordType
   * @return optional with mapping rules
   */
  public Future<Optional<JsonObject>> get(MappingRuleCacheKey key) {
    Promise<Optional<JsonObject>> promise = Promise.promise();
    cache.get(key).whenComplete((rulesOptional, e) -> {
      if (e == null) {
        promise.complete(rulesOptional);
      } else {
        promise.fail(e);
      }
    });
    return promise.future();
  }

  /**
   * Saves mapping rules in this cache for the specified tenant id
   * @param key contains tenantId and recordType
   * @param mappingRules  mapping rules
   */
  public void put(MappingRuleCacheKey key, JsonObject mappingRules) {
    cache.put(key, CompletableFuture.completedFuture(Optional.of(mappingRules)));
  }
}
