package org.folio.services;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Cache;
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
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

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

  public MappingMetadataServiceImpl(@Autowired MappingParametersProvider mappingParametersProvider,
                                    @Autowired MappingRuleService mappingRuleService,
                                    @Autowired MappingRulesSnapshotDao mappingRulesSnapshotDao,
                                    @Autowired MappingParamsSnapshotDao mappingParamsSnapshotDao,
                                    @Value("${srm.metadata.cache.expiration.seconds:3600}") long cacheExpirationTime,
                                    @Value("${srm.metadata.cache.max.size:20}") int cacheMaxSize) {

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
  public Future<MappingMetadataDto> getMappingMetadataDto(String jobExecutionId, OkapiConnectionParams okapiParams) {
    LOGGER.info("getMappingMetadataDto:: Retrieving MappingMetadataDto for jobExecutionId: '{}'", jobExecutionId);
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

        logCacheStats(mappingParamsCache, "MappingParametersCache");
        logCacheStats(mappingRulesCache, "MappingRulesCache");

        return Future.succeededFuture(new MappingMetadataDto()
          .withJobExecutionId(jobExecutionId)
          .withMappingParams(Json.encode(params))
          .withMappingRules(rules.encode()));
      });
  }

  private CompletableFuture<MappingParameters> loadMappingParams(String jobExecutionId, OkapiConnectionParams okapiParams) {
    return retrieveMappingParameters(jobExecutionId, okapiParams)
      .onFailure(throwable -> LOGGER.error("loadMappingParams:: Failed to load mapping parameters for jobExecutionId: '{}'", jobExecutionId, throwable))
      .toCompletionStage()
      .toCompletableFuture();
  }

  private CompletableFuture<JsonObject> loadMappingRules(String jobExecutionId, String tenantId) {
    return retrieveMappingRules(jobExecutionId, tenantId)
      .toCompletionStage()
      .toCompletableFuture();
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
    return mappingParametersProvider.get(jobExecutionId, okapiParams)
      .compose(mappingParameters -> {
        LOGGER.debug("Attempting to save MappingParameters snapshot to DB for jobExecutionId: '{}'", jobExecutionId);
        return mappingParamsSnapshotDao.save(mappingParameters, jobExecutionId, okapiParams.getTenantId())
          .map(mappingParameters);
      })
      .onSuccess(mappingParameters -> {
        if (mappingParameters != null) {
          LOGGER.debug("Successfully saved MappingParameters snapshot to DB for jobExecutionId: '{}'. Updating cache.", jobExecutionId);
          mappingParamsCache.put(jobExecutionId, CompletableFuture.completedFuture(mappingParameters));
        }
      }).onFailure(throwable -> LOGGER.error("Failed to save MappingParameters snapshot for jobExecutionId: '{}'", jobExecutionId, throwable));
  }

  @Override
  public Future<JsonObject> saveMappingRulesSnapshot(String jobExecutionId, String recordType, String tenantId) {
    return mappingRuleService.get(Record.RecordType.fromValue(recordType), tenantId)
      .map(rulesOptional -> rulesOptional.orElseThrow(() ->
        new NotFoundException(String.format("Mapping rules are not found for tenant id '%s'", tenantId))))
      .compose(rules -> {
        LOGGER.debug("Attempting to save MappingRules to DB for jobExecutionId: '{}'", jobExecutionId);
        return mappingRulesSnapshotDao.save(rules, jobExecutionId, tenantId)
          .map(rules);
      }).onSuccess(mappingRules -> {
        if (mappingRules != null) {
          LOGGER.debug("Successfully saved MappingRules to DB for jobExecutionId: '{}'. Updating cache.", jobExecutionId);
          mappingRulesCache.put(jobExecutionId, CompletableFuture.completedFuture(mappingRules));
        }
      }).onFailure(throwable -> LOGGER.error("Failed to save MappingRules for jobExecutionId: '{}'", jobExecutionId, throwable));
  }

  private Future<MappingParameters> retrieveMappingParameters(String jobExecutionId, OkapiConnectionParams okapiParams) {
    return mappingParamsSnapshotDao.getByJobExecutionId(jobExecutionId, okapiParams.getTenantId())
      .map(mappingParamsOptional -> mappingParamsOptional.orElseThrow(() ->
        new NotFoundException(String.format("Mapping parameters snapshot is not found for JobExecution '%s'", jobExecutionId))));
  }

  private Future<JsonObject> retrieveMappingRules(String jobExecutionId, String tenantId) {
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
