package org.folio.services;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.folio.Record;
import org.folio.dao.MappingParamsSnapshotDao;
import org.folio.dao.MappingRulesSnapshotDao;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.MappingMetadataDto;
import org.folio.services.mappers.processor.MappingParametersProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

@Log4j2
@Service
public class MappingMetadataServiceImpl implements MappingMetadataService {

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

  public void logCacheStats(AsyncCache<?, ?> cache, String cacheName) {
    CacheStats stats = cache.synchronous().stats();
    log.debug("Cache {} statistics :", cacheName);
    log.debug("  Request Count: {}", stats.requestCount());
    log.debug("  Hit Count: {}", stats.hitCount());
    log.debug("  Hit Rate: {}%", String.format("%.2f", stats.hitRate() * 100));
    log.debug("  Miss Count: {}", stats.missCount());
    log.debug("  Miss Rate: {}%", String.format("%.2f", stats.missRate() * 100));
    log.debug("  Load Count: {}", stats.loadCount());
    log.debug("  Average Load Time: {}%", String.format("%.2f", stats.averageLoadPenalty() / 1_000_000.0));
    log.debug("  Eviction Count: {}", stats.evictionCount());
  }

  @Override
  public Future<MappingMetadataDto> getMappingMetadataDto(String jobExecutionId, ConnectionParams okapiParams) {
    log.debug("getMappingMetadataDto:: Starting request for jobExecutionId: '{}'", jobExecutionId);

    Future<MappingParameters> mappingParamsFuture = Future.fromCompletionStage(
      mappingParamsCache.get(jobExecutionId, (key, executor) -> loadMappingParams(key, okapiParams))
    );

    Future<JsonObject> mappingRulesFuture = Future.fromCompletionStage(
      mappingRulesCache.get(jobExecutionId, (key, executor) -> loadMappingRules(key, okapiParams.getTenantId()))
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
        log.debug("getMappingMetadataDto:: Completed request for jobExecutionId: '{}'", jobExecutionId);
        if (log.isDebugEnabled()) {
          logCacheStats(mappingParamsCache, "MappingParametersCache");
          logCacheStats(mappingRulesCache, "MappingRulesCache");
        }
      });
  }

  private CompletableFuture<MappingParameters> loadMappingParams(String jobExecutionId, ConnectionParams okapiParams) {
    log.debug("loadMappingParams:: Loading Mapping Params from source for jobExecutionId: '{}'", jobExecutionId);
    return retrieveMappingParameters(jobExecutionId, okapiParams)
      .onFailure(t -> {
        if (!(t instanceof NotFoundException)) {
          log.error("loadMappingParams:: Failed to load mapping parameters for jobExecutionId: '{}'", jobExecutionId, t);
        } else {
          log.warn("loadMappingParams:: Mapping parameters not found for jobExecutionId: '{}'", jobExecutionId);
        }
      })
      .toCompletionStage()
      .toCompletableFuture();
  }

  private CompletableFuture<JsonObject> loadMappingRules(String jobExecutionId, String tenantId) {
    log.debug("loadMappingRules:: Loading Mapping Rules from source for jobExecutionId: '{}'", jobExecutionId);
    return retrieveMappingRules(jobExecutionId, tenantId)
      .onFailure(t -> {
        if (!(t instanceof NotFoundException)) {
          log.error("loadMappingRules:: Failed to load mapping rules for jobExecutionId: '{}'", jobExecutionId, t);
        } else {
          log.warn("loadMappingRules:: Mapping rules not found for jobExecutionId: '{}'", jobExecutionId);
        }
      })
      .toCompletionStage()
      .toCompletableFuture();
  }

  @Override
  public Future<MappingMetadataDto> getMappingMetadataDtoByRecordType(Record.RecordType recordType,
                                                                      ConnectionParams okapiParams) {
    return Future.all(mappingParametersProvider.get(recordType.value(), okapiParams),
      retrieveMappingRulesByRecordType(recordType, okapiParams.getTenantId()))
        .compose(res -> Future.succeededFuture(new MappingMetadataDto()
          .withMappingParams(Json.encode(res.resultAt(0)))
          .withMappingRules(((JsonObject) res.resultAt(1)).encode())));
  }

  @Override
  public Future<MappingParameters> saveMappingParametersSnapshot(String jobExecutionId, ConnectionParams okapiParams) {
    log.debug("saveMappingParametersSnapshot:: Saving MappingParameters snapshot for jobExecutionId: '{}'", jobExecutionId);
    return mappingParametersProvider.get(jobExecutionId, okapiParams)
      .compose(mappingParameters -> {
        log.debug("Attempting to save MappingParameters snapshot to DB for jobExecutionId: '{}'", jobExecutionId);
        return mappingParamsSnapshotDao.save(mappingParameters, jobExecutionId, okapiParams.getTenantId())
          .map(mappingParameters);
      })
      .onSuccess(mappingParameters -> {
        if (mappingParameters != null) {
          log.debug("Successfully saved MappingParameters snapshot to DB for jobExecutionId: '{}'. Updating cache.", jobExecutionId);
          mappingParamsCache.put(jobExecutionId, CompletableFuture.completedFuture(mappingParameters));
        }
      }).onFailure(throwable -> log.error("Failed to save MappingParameters snapshot for jobExecutionId: '{}'", jobExecutionId, throwable));
  }

  @Override
  public Future<JsonObject> saveMappingRulesSnapshot(String jobExecutionId, String recordType, String tenantId) {
    log.debug("saveMappingRulesSnapshot:: Saving MappingRules snapshot for jobExecutionId: '{}', recordType: '{}', tenantId: '{}'",
      jobExecutionId, recordType, tenantId);

    return mappingRuleService.get(Record.RecordType.fromValue(recordType), tenantId)
      .map(rulesOptional -> rulesOptional.orElseThrow(() ->
        new NotFoundException(String.format("Mapping rules are not found for tenant id '%s'", tenantId))))
      .compose(rules -> {
        log.debug("Attempting to save MappingRules to DB for jobExecutionId: '{}'", jobExecutionId);
        return mappingRulesSnapshotDao.save(rules, jobExecutionId, tenantId)
          .map(rules);
      }).onSuccess(mappingRules -> {
        if (mappingRules != null) {
          log.debug("Successfully saved MappingRules to DB for jobExecutionId: '{}'. Updating cache.", jobExecutionId);
          mappingRulesCache.put(jobExecutionId, CompletableFuture.completedFuture(mappingRules));
        }
      }).onFailure(throwable -> log.error("Failed to save MappingRules for jobExecutionId: '{}'", jobExecutionId, throwable));
  }

  private Future<MappingParameters> retrieveMappingParameters(String jobExecutionId, ConnectionParams okapiParams) {
    log.debug("retrieveMappingParameters:: Retrieving MappingParameters snapshot for jobExecutionId: '{}'", jobExecutionId);
    return mappingParamsSnapshotDao.getByJobExecutionId(jobExecutionId, okapiParams.getTenantId())
      .map(mappingParamsOptional -> mappingParamsOptional.orElseThrow(() ->
        new NotFoundException(String.format("Mapping parameters snapshot is not found for JobExecution '%s'", jobExecutionId))));
  }

  private Future<JsonObject> retrieveMappingRules(String jobExecutionId, String tenantId) {
    log.debug("retrieveMappingRules:: Retrieving MappingRules snapshot for jobExecutionId: '{}'", jobExecutionId);
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
