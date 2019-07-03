package org.folio.dao.util;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.Progress;

/**
 * Functional interface for changing JobExecution Progress in blocking update statement
 */
@FunctionalInterface
public interface JobExecutionProgressMutator {

  /**
   * @param progress loaded from the db Progress
   * @return future with updated Progress ready to be saved in the db
   */
  Future<Progress> mutate(Progress progress);

}
