package org.folio.services;

import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.ParsedRecordDto;

import io.vertx.core.Future;

/**
 * Service for processing parsed record
 */
public interface ParsedRecordService {

  /**
   * Get {@link ParsedRecordDto} by instanceId
   * @param instanceId - instanceId
   * @param params - OkapiConnectionParams
   * @return future with ParsedRecordDto, which was mapped from SourceRecord from SRS
   */
  Future<ParsedRecordDto> getRecordByInstanceId(String instanceId, OkapiConnectionParams params);
}
