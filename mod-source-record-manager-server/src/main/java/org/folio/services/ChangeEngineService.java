package org.folio.services;

import io.vertx.core.Future;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;

import java.util.List;

public interface ChangeEngineService {

  /**
   * Parse raw records and save parsed result or errors to mod-source-storage
   *
   * @param chunk         - {@link RawRecordsDto} chunk with list of raw records for parse
   * @param jobExecution  - JobExecution which records should be parsed
   * @param sourceChunkId - id of the JobExecutionSourceChunk
   * @param params        - OkapiConnectionParams to interact with external services
   * @return - list of Records that was parsed and saved into the source-record-storage
   */
  Future<List<Record>> parseRawRecordsChunkForJobExecution(RawRecordsDto chunk, JobExecution jobExecution, String sourceChunkId, OkapiConnectionParams params);
}
