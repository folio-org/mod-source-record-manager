package org.folio.services.afterprocessing;

import io.vertx.core.Future;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.Record;

import java.util.List;

public interface AfterProcessingService {

  /**
   * Provides further processing of Parsed Records
   *
   * @param record       -  parsed records for processing
   * @param sourceChunkId - id of the JobExecutionSourceChunk
   * @param params        - OkapiConnectionParams to interact with external services
   * @return future
   */
  Future<Void> process(List<Record> record, String sourceChunkId, OkapiConnectionParams params);
}
