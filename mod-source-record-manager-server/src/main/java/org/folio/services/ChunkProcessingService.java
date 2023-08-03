package org.folio.services;

import io.vertx.core.Future;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.RawRecordsDto;

public interface ChunkProcessingService {

  /**
   * Process chunk of RawRecords
   *
   * @param chunk          - {@link RawRecordsDto} chunk with list of raw records
   * @param jobExecutionId - JobExecution id
   * @param acceptInstanceId - allow the 999ff$i field to be set and also create an instance with value in 999ff$i
   * @param params         - OkapiConnectionParams to interact with external services
   * @return - true if chunk was processed correctly
   */
  Future<Boolean> processChunk(RawRecordsDto chunk, String jobExecutionId, boolean acceptInstanceId, OkapiConnectionParams params);

  Future<Boolean> processChunk(RawRecordsDto incomingChunk, JobExecution jobExecution, OkapiConnectionParams params);

}
