package org.folio.dao.util;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.JobExecution;

/**
 * Functional interface for changing JobExecution in blocking update statement
 */
@FunctionalInterface
public interface JobExecutionMutator {

  /**
   * @param jobExecution loaded from the db JobExecution
   * @return future with updated JobExecution ready to be saved in the db
   */
  Future<JobExecution> mutate(JobExecution jobExecution);

}
