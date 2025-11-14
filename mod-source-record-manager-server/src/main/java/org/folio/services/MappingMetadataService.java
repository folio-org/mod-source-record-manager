package org.folio.services;

import io.vertx.core.Future;
import org.folio.Record;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.MappingMetadataDto;
import io.vertx.core.json.JsonObject;

/**
 * Service for managing Mapping Rules and Parameters
 */
public interface MappingMetadataService {

  /**
   * Returns Mapping rules and Mapping parameters in MappingMetadataDto entity
   *
   * @param jobExecutionId jobExecution id
   * @param okapiParams    okapi connection params
   * @return MappingMetadataDto
   */
  Future<MappingMetadataDto> getMappingMetadataDto(String jobExecutionId, OkapiConnectionParams okapiParams);

  /**
   * Returns Mapping rules and Mapping parameters in MappingMetadataDto entity by recordType
   *
   * @param recordType  type of rules (e.g. MARC_BIB or MARK_HOLDING)
   * @param okapiParams  okapi connection params
   * @return MappingMetadataDto
   */
  Future<MappingMetadataDto> getMappingMetadataDtoByRecordType(Record.RecordType recordType, OkapiConnectionParams okapiParams);

  /**
   * Creates a snapshot of Mapping parameters and saves it to DB
   *
   * @param jobExecutionId jobExecution id
   * @param okapiParams    okapi connection params
   * @return Mapping Parameters snapshot
   */
  Future<MappingParameters> saveMappingParametersSnapshot(String jobExecutionId, OkapiConnectionParams okapiParams);

  /**
   * Creates a snapshot of Mapping rules and saves it to DB
   *
   * @param jobExecutionId jobExecution id
   * @param recordType     record type
   * @param tenantId       tenantId
   * @return Mapping rules snapshot
   */
  Future<JsonObject> saveMappingRulesSnapshot(String jobExecutionId, String recordType,  String tenantId);

}
