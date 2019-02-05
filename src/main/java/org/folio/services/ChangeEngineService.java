package org.folio.services;

import io.vertx.core.Future;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.JobExecution;

public interface ChangeEngineService {

  /**
   * Load source records from mod-source-storage, parse them and save parsed result or errors to mod-source-storage
   *
   * @param job    - JobExecution which records should be parsed
   * @param params - OkapiConnectionParams to interact with external services
   * @return - JobExecution with changed status and saved parsed records for it
   */
  Future<JobExecution> parseSourceRecordsForJobExecution(JobExecution job, OkapiConnectionParams params);
}
