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
  Future<Boolean> processChunk(RawRecordsDto chunk, String jobExecutionId, OkapiConnectionParams params);

  /**
   * Process and send events after parsed records were stored in SRS
   *
   * @param createdRecords - records that were stored in SRS
   * @param jobExecutionId - JobExecution id
   * @param params         - OkapiConnectionParams to interact with external services
   * @return - true if operation was successful
   */
  Future<Boolean> sendEventsWithStoredRecords(List<Record> createdRecords, String jobExecutionId, OkapiConnectionParams params);
}
