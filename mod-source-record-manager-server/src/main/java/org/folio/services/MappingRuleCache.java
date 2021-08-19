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

import org.folio.Record;
import org.folio.dao.MappingRuleDao;

/**
 * In-memory cache for the mapping rules
 */
@Component
public class MappingRuleCache {

  private static final Logger LOGGER = LogManager.getLogger();

  private MappingRuleDao mappingRuleDao;
  private AsyncLoadingCache<String, Optional<JsonObject>> cache;

  @Autowired
  public MappingRuleCache(MappingRuleDao mappingRuleDao, Vertx vertx) {
    this.mappingRuleDao = mappingRuleDao;
    cache = Caffeine.newBuilder()
      .executor(task -> vertx.runOnContext(ar -> task.run()))
      .buildAsync((key, executor) -> loadMappingRules(key, executor, mappingRuleDao));
  }

  //TODO refactor to support MARC_HOLDING rules https://issues.folio.org/browse/MODSOURMAN-547
  private CompletableFuture<Optional<JsonObject>> loadMappingRules(String tenantId, Executor executor, MappingRuleDao mappingRuleDao) {
    CompletableFuture<Optional<JsonObject>> future = new CompletableFuture<>();
    executor.execute(() -> mappingRuleDao.get(Record.RecordType.MARC_BIB, tenantId).onComplete(ar -> {
      if (ar.failed()) {
        LOGGER.error("Failed to load mapping rules for tenant '{}' from data base", tenantId, ar.cause());
        future.completeExceptionally(ar.cause());
        return;
      }
      future.complete(ar.result());
    }));
    return future;
  }

  /**
   * Returns mapping rules associated with specified tenant id
   * @param  tenantId tenant id
   * @return optional with mapping rules
   */
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

  /**
   * Saves mapping rules in this cache for the specified tenant id
   * @param tenantId      tenant id
   * @param mappingRules  mapping rules
   */
  public void put(String tenantId, JsonObject mappingRules) {
    cache.put(tenantId, CompletableFuture.completedFuture(Optional.of(mappingRules)));
  }
}
