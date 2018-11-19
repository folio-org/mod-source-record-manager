package org.folio.dao;

import io.vertx.core.Vertx;
import org.folio.rest.jaxrs.model.JobExecution;

/**
 * Implementation for the JobExecutionDao, works with PostgresClient to access the data.
 *
 * @see JobExecution
 * @see JobExecutionDao
 * @see org.folio.rest.persist.PostgresClient
 */
public class JobExecutionDaoImpl extends AbstractGenericDao<JobExecution> implements JobExecutionDao {

  private static final String TABLE_NAME = "job_executions";
  private static final String SCHEMA_PATH = "ramls/jobExecution.json";

  public JobExecutionDaoImpl(Vertx vertx, String tenantId) {
    super(vertx, tenantId, JobExecution.class);
  }

  @Override
  protected String getTableName() {
    return TABLE_NAME;
  }

  @Override
  protected String getSchemaPath() {
    return SCHEMA_PATH;
  }
}
