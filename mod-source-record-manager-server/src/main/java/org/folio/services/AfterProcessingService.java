package org.folio.services;

import io.vertx.core.Future;
import org.folio.dataimport.util.OkapiConnectionParams;

public interface AfterProcessingService {

  /**
   * Provides further processing of Parsed Records
   *
   * @param records       - list of parsed records
   * @param sourceChunkId - id of the JobExecutionSourceChunk
   * @param params        - OkapiConnectionParams to interact with external services
   * @return RecordProcessingContext - context object with records and properties
   */
  Future<RecordProcessingContext> process(RecordProcessingContext records, String sourceChunkId, OkapiConnectionParams params);
}
