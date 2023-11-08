package org.folio.services;

import io.vertx.core.Future;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;

import java.util.List;

public interface ChangeEngineService {

  /**
   * Parse raw records and save parsed result or errors to mod-source--record-storage
   *
   * @param chunk         - {@link RawRecordsDto} chunk with list of raw records for parse
   * @param jobExecution  - JobExecution which records should be parsed
   * @param sourceChunkId - id of the JobExecutionSourceChunk
   * @param acceptInstanceId - allow the 999ff$i field to be set and also create an instance with value in 999ff$i
   * @param params        - OkapiConnectionParams to interact with external services
   * @return - list of Records that was parsed and saved into the source-record-storage
   */
  Future<List<Record>> parseRawRecordsChunkForJobExecution(RawRecordsDto chunk,
                                                           JobExecution jobExecution, String sourceChunkId,
                                                           boolean acceptInstanceId,
                                                           OkapiConnectionParams params);
}
