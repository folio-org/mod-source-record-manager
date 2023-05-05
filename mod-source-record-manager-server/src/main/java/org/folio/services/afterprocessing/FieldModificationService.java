package org.folio.services.afterprocessing;

import io.vertx.core.Future;
import java.util.List;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.Record;

public interface FieldModificationService {

  /**
   * Remove $9 subfields from controllable fields
   *
   * @param jobExecutionId - Job execution id
   * @param folioRecords - list of parsed MARC records
   * @param okapiParams - Okapi connection params
   */
  Future<List<Record>> remove9Subfields(String jobExecutionId, List<Record> folioRecords, OkapiConnectionParams okapiParams);

}
