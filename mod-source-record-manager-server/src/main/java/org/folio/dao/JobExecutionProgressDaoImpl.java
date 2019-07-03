package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dao.util.JobExecutionMutator;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.Progress;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * Implementation for the JobExecutionProgressDao, works with PostgresClient to access data.
 *
 * @see Progress
 * @see JobExecution
 * @see JobExecutionProgressDao
 * @see org.folio.rest.persist.PostgresClient
 */
@Repository
public class JobExecutionProgressDaoImpl implements JobExecutionProgressDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutionProgressDaoImpl.class);

  private static final String TABLE_NAME = "job_execution_progress";
  private static final String ID_FIELD = "'id'";


  @Override
  public Future<String> save(Progress progress, String tenantId) {
    return null;
  }

  @Override
  public Future<Progress> updateBlocking(String jobExecutionId, JobExecutionMutator mutator, String tenantId) {
    return null;
  }

  @Override
  public Future<Optional<Progress>> getProgressByJobExecutionId(String jobExecutionId, String tenantId) {
    return null;
  }
}
