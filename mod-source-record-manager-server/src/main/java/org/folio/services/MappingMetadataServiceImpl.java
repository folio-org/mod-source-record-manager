package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import org.folio.Record;
import org.folio.dao.MappingParamsSnapshotDao;
import org.folio.dao.MappingRulesSnapshotDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.MappingMetadataDto;
import org.folio.services.mappers.processor.MappingParametersProvider;
import org.springframework.beans.factory.annotation.Autowired;
import io.vertx.core.json.JsonObject;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;

@Service
public class MappingMetadataServiceImpl implements MappingMetadataService {

  private final MappingParametersProvider mappingParametersProvider;
  private final MappingRuleService mappingRuleService;
  private final MappingRulesSnapshotDao mappingRulesSnapshotDao;
  private final MappingParamsSnapshotDao mappingParamsSnapshotDao;

  public MappingMetadataServiceImpl(@Autowired MappingParametersProvider mappingParametersProvider,
                                    @Autowired MappingRuleService mappingRuleService,
                                    @Autowired MappingRulesSnapshotDao mappingRulesSnapshotDao,
                                    @Autowired MappingParamsSnapshotDao mappingParamsSnapshotDao) {
    this.mappingParametersProvider = mappingParametersProvider;
    this.mappingRuleService = mappingRuleService;
    this.mappingRulesSnapshotDao = mappingRulesSnapshotDao;
    this.mappingParamsSnapshotDao = mappingParamsSnapshotDao;
  }

  @Override
  public Future<MappingMetadataDto> getMappingMetadataDto(String jobExecutionId, OkapiConnectionParams okapiParams) {
    return retrieveMappingParameters(jobExecutionId, okapiParams)
      .compose(mappingParameters ->
        retrieveMappingRules(jobExecutionId, okapiParams.getTenantId())
          .compose(mappingRules -> Future.succeededFuture(
            new MappingMetadataDto()
              .withJobExecutionId(jobExecutionId)
              .withMappingParams(Json.encode(mappingParameters))
              .withMappingRules(mappingRules.encode()))));
  }

  @Override
  public Future<MappingParameters> saveMappingParametersSnapshot(String jobExecutionId, OkapiConnectionParams okapiParams) {
    return mappingParametersProvider.get(jobExecutionId, okapiParams)
      .compose(mappingParameters -> mappingParamsSnapshotDao.save(mappingParameters, jobExecutionId, okapiParams.getTenantId())
        .map(mappingParameters));
  }

  @Override
  public Future<JsonObject> saveMappingRulesSnapshot(String jobExecutionId, String recordType, String tenantId) {
    return mappingRuleService.get(Record.RecordType.fromValue(recordType), tenantId)
      .map(rulesOptional -> rulesOptional.orElseThrow(() ->
        new NotFoundException(String.format("Mapping rules are not found for tenant id '%s'", tenantId))))
      .compose(rules -> mappingRulesSnapshotDao.save(rules, jobExecutionId, tenantId)
        .map(rules));
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
}
