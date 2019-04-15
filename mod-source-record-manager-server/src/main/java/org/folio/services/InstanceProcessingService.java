package org.folio.services;

import io.vertx.core.Future;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.rest.jaxrs.model.Record;

import java.util.List;

public interface InstanceProcessingService {

  /**
   * Maps list of parsed records to Instances and posts them to the mod-inventory
   *
   * @param records - list of parsed records
   * @param sourceChunkId - id of the JobExecutionSourceChunk
   * @param params - OkapiConnectionParams to interact with external services
   * @return list of Instance records
   */
  Future<List<Instance>> mapRecordsToInstances(List<Record> records, String sourceChunkId, OkapiConnectionParams params);
}
