package org.folio.services;

import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.ParsedRecordDto;

import io.vertx.core.Future;

/**
 * Service for processing parsed record
 */
public interface ParsedRecordService {

  /**
   * Get MarcRecord (ParsedRecordDto) by intanceId
   * @param instanceId - instanceId
   * @param params - OkapiConnectionParams
   * @return future with ParsedRecordDto, which was mapped from SourceRecord from srs.
   */
  Future<ParsedRecordDto> getMarcRecordByInstanceId(String instanceId, OkapiConnectionParams params);
}
