package org.folio.services;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.folio.Record;
import org.folio.dao.MappingParamsSnapshotDao;
import org.folio.dao.MappingRulesSnapshotDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.MappingMetadataDto;
import org.folio.services.mappers.processor.MappingParametersProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;

@Service
public class MappingMetadataServiceImpl implements MappingMetadataService {

  private final MappingParametersProvider mappingParametersProvider;
  private final MappingRuleService mappingRuleService;
  private final MappingRulesSnapshotDao mappingRulesSnapshotDao;
  private final MappingParamsSnapshotDao mappingParamsSnapshotDao;
  private final Cache<String, MappingParameters> mappingParamsCache;
  private final Cache<String, JsonObject> mappingRulesCache;
  private final Executor cacheExecutor = serviceExecutor -> {
    Context context = Vertx.currentContext();
    if (context != null) {
      context.runOnContext(ar -> serviceExecutor.run());
    } else {
      // The common pool below is used because it is the  default executor for caffeine
      ForkJoinPool.commonPool().execute(serviceExecutor);
    }
  };

  public MappingMetadataServiceImpl(@Autowired MappingParametersProvider mappingParametersProvider,
                                    @Autowired MappingRuleService mappingRuleService,
                                    @Autowired MappingRulesSnapshotDao mappingRulesSnapshotDao,
                                    @Autowired MappingParamsSnapshotDao mappingParamsSnapshotDao) {
    this.mappingParametersProvider = mappingParametersProvider;
    this.mappingRuleService = mappingRuleService;
    this.mappingRulesSnapshotDao = mappingRulesSnapshotDao;
    this.mappingParamsSnapshotDao = mappingParamsSnapshotDao;

    this.mappingParamsCache = Caffeine.newBuilder()
      .maximumSize(20)
      .executor(cacheExecutor)
      .build();

    this.mappingRulesCache = Caffeine.newBuilder()
      .maximumSize(20)
      .executor(cacheExecutor)
      .build();
  }

  @Override
  public Future<MappingMetadataDto> getMappingMetadataDto(String jobExecutionId, OkapiConnectionParams okapiParams) {
    MappingParameters cachedMappingParams = mappingParamsCache.getIfPresent(jobExecutionId);
    JsonObject cacheMappingRules = mappingRulesCache.getIfPresent(jobExecutionId);
    if (cacheMappingRules == null || cachedMappingParams == null) {
      return CompositeFuture.all(retrieveMappingParameters(jobExecutionId, okapiParams),
          retrieveMappingRules(jobExecutionId, okapiParams.getTenantId()))
        .compose(res -> Future.succeededFuture(new MappingMetadataDto()
          .withJobExecutionId(jobExecutionId)
          .withMappingParams(Json.encode(res.resultAt(0)))
          .withMappingRules(((JsonObject) res.resultAt(1)).encode())));
    }
    return Future.succeededFuture(new MappingMetadataDto()
      .withJobExecutionId(jobExecutionId)
      .withMappingParams(Json.encode(cachedMappingParams))
      .withMappingRules((cacheMappingRules.encode())));
  }

  @Override
  public Future<MappingMetadataDto> getMappingMetadataDtoByRecordType(Record.RecordType recordType,
                                                                      OkapiConnectionParams okapiParams) {
    return CompositeFuture.all(mappingParametersProvider.get(recordType.value(), okapiParams),
      retrieveMappingRulesByRecordType(recordType, okapiParams.getTenantId()))
        .compose(res -> Future.succeededFuture(new MappingMetadataDto()
          .withMappingParams(Json.encode(res.resultAt(0)))
          .withMappingRules(((JsonObject) res.resultAt(1)).encode())));
  }

  @Override
  public Future<MappingParameters> saveMappingParametersSnapshot(String jobExecutionId, OkapiConnectionParams okapiParams) {
    return mappingParametersProvider.get(jobExecutionId, okapiParams)
      .compose(mappingParameters -> mappingParamsSnapshotDao.save(mappingParameters, jobExecutionId, okapiParams.getTenantId())
        .map(mappingParameters))
      .onSuccess(mappingParameters -> {
        if (mappingParameters != null) {
          mappingParamsCache.put(jobExecutionId, mappingParameters);
        }
      });
  }

  @Override
  public Future<JsonObject> saveMappingRulesSnapshot(String jobExecutionId, String recordType, String tenantId) {
    return mappingRuleService.get(Record.RecordType.fromValue(recordType), tenantId)
      .map(rulesOptional -> rulesOptional.orElseThrow(() ->
        new NotFoundException(String.format("Mapping rules are not found for tenant id '%s'", tenantId))))
      .compose(rules -> mappingRulesSnapshotDao.save(rules, jobExecutionId, tenantId)
        .map(rules))
      .onSuccess(mappingRules -> {
        if (mappingRules != null) {
          mappingRulesCache.put(jobExecutionId, mappingRules);
        }
      });
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
