package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;

import static org.folio.dao.util.DaoUtil.getCQLWrapper;

/**
 * Implementation for the JobExecutionDao, works with PostgresClient to access the data.
 *
 * @see JobExecution
 * @see JobExecutionDao
 * @see org.folio.rest.persist.PostgresClient
 */
public class JobExecutionDaoImpl implements JobExecutionDao {

  private static final Logger LOG = LoggerFactory.getLogger(JobExecutionDaoImpl.class);

  public static final String TABLE_NAME = "job_executions";
  private PostgresClient pgClient;

  public JobExecutionDaoImpl(Vertx vertx, String tenantId) {
    this.pgClient = PostgresClient.getInstance(vertx, tenantId);
  }

  @Override
  public Future<JobExecutionCollection> getJobExecutions(String query, int offset, int limit) {
    Future<Results<JobExecution>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cql = getCQLWrapper(TABLE_NAME, query, limit, offset);
      pgClient.get(TABLE_NAME, JobExecution.class, fieldList, cql, true, false, future.completer());
    } catch (Exception e) {
      LOG.error("Error while getting JobExecutions", e);
      future.fail(e);
    }
    return future.map(results -> new JobExecutionCollection()
      .withJobExecutions(results.getResults())
      .withTotalRecords(results.getResultInfo().getTotalRecords()));
  }

  @Override
  public Future<String> save(JobExecution jobExecution) {
    Future<String> future = Future.future();
    pgClient.save(TABLE_NAME, jobExecution.getId(), jobExecution, future.completer());
    return future;
  }
}
