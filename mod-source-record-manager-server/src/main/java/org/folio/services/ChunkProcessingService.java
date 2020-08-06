package org.folio.services;

import io.vertx.core.Future;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.Record;

import java.util.List;

public interface ChunkProcessingService {

  /**
   * Process chunk of RawRecords
   *
   * @param chunk          - {@link RawRecordsDto} chunk with list of raw records
   * @param jobExecutionId - JobExecution id
   * @param params         - OkapiConnectionParams to interact with external services
   * @return - true if chunk was processed correctly
   */
  Future<Boolean> startChunkProcessing(RawRecordsDto chunk, String jobExecutionId, OkapiConnectionParams params);
  Future<Boolean> sendEventsWithStoredRecords(List<Record> createdRecords, String jobExecutionId, OkapiConnectionParams params);
}
