package org.folio.services;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.Record;
import org.folio.dao.MappingParamsSnapshotDao;
import org.folio.dao.MappingRulesSnapshotDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.MappingMetadataDto;
import org.folio.services.mappers.processor.MappingParametersProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class MappingMetadataServiceImpl implements MappingMetadataService {

  private static final Logger LOGGER = LogManager.getLogger();

  private final MappingParametersProvider mappingParametersProvider;
  private final MappingRuleService mappingRuleService;
  private final MappingRulesSnapshotDao mappingRulesSnapshotDao;
  private final MappingParamsSnapshotDao mappingParamsSnapshotDao;
  private final AsyncCache<String, MappingParameters> mappingParamsCache;
  private final AsyncCache<String, JsonObject> mappingRulesCache;
  private final Executor cacheExecutor = serviceExecutor -> {
    Context context = Vertx.currentContext();
    if (context != null) {
      context.runOnContext(ar -> serviceExecutor.run());
    } else {
      // The common pool below is used because it is the default executor for caffeine
      ForkJoinPool.commonPool().execute(serviceExecutor);
    }
  };

  private final AtomicInteger concurrentRequests = new AtomicInteger(0);
  private final ConcurrentHashMap<String, CompletableFuture<MappingParameters>> loadingParams = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, CompletableFuture<JsonObject>> loadingRules = new ConcurrentHashMap<>();

  public MappingMetadataServiceImpl(@Autowired MappingParametersProvider mappingParametersProvider,
                                    @Autowired MappingRuleService mappingRuleService,
                                    @Autowired MappingRulesSnapshotDao mappingRulesSnapshotDao,
                                    @Autowired MappingParamsSnapshotDao mappingParamsSnapshotDao,
                                    @Value("${srm.metadata.cache.expiration.seconds:3600}") long cacheExpirationTime,
                                    @Value("${srm.metadata.cache.max.size:200}") int cacheMaxSize) {

    this.mappingParametersProvider = mappingParametersProvider;
    this.mappingRuleService = mappingRuleService;
    this.mappingRulesSnapshotDao = mappingRulesSnapshotDao;
    this.mappingParamsSnapshotDao = mappingParamsSnapshotDao;

    this.mappingParamsCache = Caffeine.newBuilder()
      .expireAfterWrite(cacheExpirationTime, TimeUnit.SECONDS)
      .maximumSize(cacheMaxSize)
      .executor(cacheExecutor)
      .recordStats()
      .buildAsync();

    this.mappingRulesCache = Caffeine.newBuilder()
      .expireAfterWrite(cacheExpirationTime, TimeUnit.SECONDS)
      .maximumSize(cacheMaxSize)
      .executor(cacheExecutor)
      .recordStats()
      .buildAsync();
  }

  public void logCacheStats(AsyncCache cache, String cacheName) {
    CacheStats stats = cache.synchronous().stats();
    LOGGER.info("Cache {} statistics :", cacheName);
    LOGGER.info("  Request Count: {}", stats.requestCount());
    LOGGER.info("  Hit Count: {}", stats.hitCount());
    LOGGER.info("  Hit Rate: {}%", String.format("%.2f", stats.hitRate() * 100));
    LOGGER.info("  Miss Count: {}", stats.missCount());
    LOGGER.info("  Miss Rate: {}%", String.format("%.2f", stats.missRate() * 100));
    LOGGER.info("  Load Count: {}", stats.loadCount());
    LOGGER.info("  Average Load Time: {}%", String.format("%.2f", stats.averageLoadPenalty() / 1_000_000.0));
    LOGGER.info("  Eviction Count: {}", stats.evictionCount());
  }

  @Override
  public Future<MappingMetadataDto> getMappingMetadataDto(String jobExecutionId, OkapiConnectionParams okapiParams, String fromMethod) {
    int current = concurrentRequests.incrementAndGet();
    LOGGER.info("getMappingMetadataDto:: Starting request for jobExecutionId: '{}', concurrent requests: {}, from method: '{}'",
      jobExecutionId, current, fromMethod);

    Future<MappingParameters> mappingParamsFuture = Future.fromCompletionStage(
      mappingParamsCache.get(jobExecutionId, (key, executor) -> loadMappingParamsWithProtection(key, okapiParams, fromMethod))
    );

    Future<JsonObject> mappingRulesFuture = Future.fromCompletionStage(
      mappingRulesCache.get(jobExecutionId, (key, executor) -> loadMappingRulesWithProtection(key, okapiParams.getTenantId(), fromMethod))
    );

    return Future.all(mappingParamsFuture, mappingRulesFuture)
      .compose(res -> {
        MappingParameters params = res.resultAt(0);
        JsonObject rules = res.resultAt(1);
        return Future.succeededFuture(new MappingMetadataDto()
          .withJobExecutionId(jobExecutionId)
          .withMappingParams(Json.encode(params))
          .withMappingRules(rules.encode()));
      })
      .onComplete(ar -> {
        int remaining = concurrentRequests.decrementAndGet();
        LOGGER.info("getMappingMetadataDto:: Completed request for jobExecutionId: '{}', remaining requests: {}",
          jobExecutionId, remaining);

        logCacheStats(mappingParamsCache, "MappingParametersCache");
        logCacheStats(mappingRulesCache, "MappingRulesCache");
      });
  }

  private CompletableFuture<MappingParameters> loadMappingParamsWithProtection(String jobExecutionId, OkapiConnectionParams okapiParams, String fromMethod) {
    return loadingParams.compute(jobExecutionId, (key, existingFuture) -> {
      if (existingFuture != null && !existingFuture.isDone()) {
        LOGGER.info("loadMappingParamsWithProtection:: Joining existing load for jobExecutionId: '{}', concurrent threads will share result", key);
        return existingFuture;
      }

      LOGGER.info("loadMappingParamsWithProtection:: Starting NEW load for jobExecutionId: '{}', method: {}", key, fromMethod);

      return retrieveMappingParameters(key, okapiParams)
        .toCompletionStage()
        .toCompletableFuture()
        .whenComplete((result, throwable) -> {
          CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS).execute(() -> {
            loadingParams.remove(key);
          });

          if (throwable == null) {
            LOGGER.info("loadMappingParamsWithProtection:: COMPLETED load for jobExecutionId: '{}', method: {}", key, fromMethod);
          } else if (isNotFoundException(throwable)) {
            LOGGER.warn("loadMappingParamsWithProtection:: NOT FOUND for jobExecutionId: '{}', method: {}", key, fromMethod);
          } else {
            LOGGER.error("loadMappingParamsWithProtection:: FAILED load for jobExecutionId: '{}', method: {}", key, fromMethod, throwable);
          }
        });
    });
  }

  private CompletableFuture<JsonObject> loadMappingRulesWithProtection(String jobExecutionId, String tenantId, String fromMethod) {
    return loadingRules.compute(jobExecutionId, (key, existingFuture) -> {
      if (existingFuture != null && !existingFuture.isDone()) {
        LOGGER.info("loadMappingRulesWithProtection:: Joining existing load for jobExecutionId: '{}', concurrent threads will share result", key);
        return existingFuture;
      }

      LOGGER.info("loadMappingRulesWithProtection:: Starting NEW load for jobExecutionId: '{}', method: {}", key, fromMethod);

      return retrieveMappingRules(key, tenantId)
        .toCompletionStage()
        .toCompletableFuture()
        .whenComplete((result, throwable) -> {
          CompletableFuture.delayedExecutor(100, TimeUnit.MILLISECONDS).execute(() -> {
            loadingRules.remove(key);
          });

          if (throwable == null) {
            LOGGER.info("loadMappingRulesWithProtection:: COMPLETED load for jobExecutionId: '{}', method: {}", key, fromMethod);
          } else if (isNotFoundException(throwable)) {
            LOGGER.warn("loadMappingRulesWithProtection:: NOT FOUND for jobExecutionId: '{}', method: {}", key, fromMethod);
          } else {
            LOGGER.error("loadMappingRulesWithProtection:: FAILED load for jobExecutionId: '{}', method: {}", key, fromMethod, throwable);
          }
        });
    });
  }

  private boolean isNotFoundException(Throwable throwable) {
    if (throwable instanceof NotFoundException) {
      return true;
    }
    Throwable cause = throwable.getCause();
    while (cause != null) {
      if (cause instanceof NotFoundException) {
        return true;
      }
      cause = cause.getCause();
    }
    return false;
  }

  @Override
  public Future<MappingMetadataDto> getMappingMetadataDtoByRecordType(Record.RecordType recordType,
                                                                      OkapiConnectionParams okapiParams) {
    return Future.all(mappingParametersProvider.get(recordType.value(), okapiParams),
      retrieveMappingRulesByRecordType(recordType, okapiParams.getTenantId()))
        .compose(res -> Future.succeededFuture(new MappingMetadataDto()
          .withMappingParams(Json.encode(res.resultAt(0)))
          .withMappingRules(((JsonObject) res.resultAt(1)).encode())));
  }

  @Override
  public Future<MappingParameters> saveMappingParametersSnapshot(String jobExecutionId, OkapiConnectionParams okapiParams) {
    LOGGER.info("saveMappingParametersSnapshot:: Saving MappingParameters snapshot for jobExecutionId: '{}'", jobExecutionId);
    return mappingParametersProvider.get(jobExecutionId, okapiParams)
      .compose(mappingParameters -> {
        LOGGER.debug("Attempting to save MappingParameters snapshot to DB for jobExecutionId: '{}'", jobExecutionId);
        return mappingParamsSnapshotDao.save(mappingParameters, jobExecutionId, okapiParams.getTenantId())
          .map(mappingParameters);
      })
      .onSuccess(mappingParameters -> {
        if (mappingParameters != null) {
          LOGGER.info("Successfully saved MappingParameters snapshot to DB for jobExecutionId: '{}'. Updating cache.", jobExecutionId);
          mappingParamsCache.put(jobExecutionId, CompletableFuture.completedFuture(mappingParameters));
        }
      }).onFailure(throwable -> LOGGER.error("Failed to save MappingParameters snapshot for jobExecutionId: '{}'", jobExecutionId, throwable));
  }

  @Override
  public Future<JsonObject> saveMappingRulesSnapshot(String jobExecutionId, String recordType, String tenantId) {
    LOGGER.info("saveMappingRulesSnapshot:: Saving MappingRules snapshot for jobExecutionId: '{}', recordType: '{}', tenantId: '{}'",
      jobExecutionId, recordType, tenantId);

    return mappingRuleService.get(Record.RecordType.fromValue(recordType), tenantId)
      .map(rulesOptional -> rulesOptional.orElseThrow(() ->
        new NotFoundException(String.format("Mapping rules are not found for tenant id '%s'", tenantId))))
      .compose(rules -> {
        LOGGER.debug("Attempting to save MappingRules to DB for jobExecutionId: '{}'", jobExecutionId);
        return mappingRulesSnapshotDao.save(rules, jobExecutionId, tenantId)
          .map(rules);
      }).onSuccess(mappingRules -> {
        if (mappingRules != null) {
          LOGGER.info("Successfully saved MappingRules to DB for jobExecutionId: '{}'. Updating cache.", jobExecutionId);
          mappingRulesCache.put(jobExecutionId, CompletableFuture.completedFuture(mappingRules));
        }
      }).onFailure(throwable -> LOGGER.error("Failed to save MappingRules for jobExecutionId: '{}'", jobExecutionId, throwable));
  }

  private Future<MappingParameters> retrieveMappingParameters(String jobExecutionId, OkapiConnectionParams okapiParams) {
    LOGGER.info("retrieveMappingParameters:: Retrieving MappingParameters snapshot for jobExecutionId: '{}'", jobExecutionId);
    return mappingParamsSnapshotDao.getByJobExecutionId(jobExecutionId, okapiParams.getTenantId())
      .map(mappingParamsOptional -> mappingParamsOptional.orElseThrow(() ->
        new NotFoundException(String.format("Mapping parameters snapshot is not found for JobExecution '%s'", jobExecutionId))));
  }

  private Future<JsonObject> retrieveMappingRules(String jobExecutionId, String tenantId) {
    LOGGER.info("retrieveMappingRules:: Retrieving MappingRules snapshot for jobExecutionId: '{}'", jobExecutionId);
    return mappingRulesSnapshotDao.getByJobExecutionId(jobExecutionId, tenantId)
      .map(rulesOptional -> rulesOptional.orElseThrow(() ->
        new NotFoundException(String.format("Mapping rules snapshot is not found for JobExecution '%s'", jobExecutionId))));
  }

  private Future<JsonObject> retrieveMappingRulesByRecordType(Record.RecordType recordType, String tenantId) {
    return mappingRuleService.get(recordType, tenantId)
      .map(rulesOptional -> rulesOptional.orElseThrow(() ->
        new NotFoundException(String.format("Mapping rules is not found for RecordType '%s'", recordType.value()))));
  }
}
