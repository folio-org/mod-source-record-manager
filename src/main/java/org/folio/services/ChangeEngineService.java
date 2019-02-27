package org.folio.services;

import io.vertx.core.Future;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.RawRecordsDto;

public interface ChangeEngineService {

  /**
   * Parse raw records and save parsed result or errors to mod-source-storage
   *
   * @param chunk  - {@link RawRecordsDto} chunk with list of raw records for parse
   * @param job    - JobExecution which records should be parsed
   * @param params - OkapiConnectionParams to interact with external services
   * @return - {@link RawRecordsDto} that was parsed and saved into the source-record-storage
   */
  Future<RawRecordsDto> parseRawRecordsChunkForJobExecution(RawRecordsDto chunk, JobExecution job, OkapiConnectionParams params);
}
