package org.folio.dao;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.annotation.PostConstruct;
import javax.ws.rs.NotFoundException;

import org.apache.commons.lang3.StringUtils;
import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.cql2pgjson.exception.FieldException;
import org.folio.cql2pgjson.model.SqlSelect;
import org.folio.dao.util.JobExecutionMutator;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.SQLConnection;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;
import org.folio.util.ResourceUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import static org.folio.dataimport.util.DaoUtil.constructCriteria;
import static org.folio.dataimport.util.DaoUtil.getCQLWrapper;
import static org.folio.rest.jaxrs.model.JobExecution.SubordinationType.CHILD;
import static org.folio.rest.jaxrs.model.JobExecution.SubordinationType.PARENT_MULTIPLE;
import static org.folio.rest.persist.PostgresClient.convertToPsqlStandard;

/**
 * Implementation for the JobExecutionDao, works with PostgresClient to access data.
 *
 * @see JobExecution
 * @see JobExecutionDao
 * @see org.folio.rest.persist.PostgresClient
 */
@Repository
public class JobExecutionDaoImpl implements JobExecutionDao {

  private static final Logger LOGGER = LogManager.getLogger();

  private static final String TABLE_NAME = "job_executions";
  private static final String ID_FIELD = "'id'";
  public static final String GET_JOB_EXECUTION_HR_ID = "SELECT nextval('%s.job_execution_hr_id_sequence')";
  public static final String GET_JOBS_WITHOUT_PARENT_MULTIPLE_QUERY_PATH = "templates/db_scripts/get_job_execution_without_parent_multiple.sql";
  public static final String TOTAL_ROWS_COLUMN = "total_rows";
  private static final String JSONB_COLUMN = "jsonb";

  private String getJobsWithoutParentMultipleSql;

  @Autowired
  private PostgresClientFactory pgClientFactory;

  @PostConstruct
  public void init() {
    getJobsWithoutParentMultipleSql = ResourceUtil.asString(GET_JOBS_WITHOUT_PARENT_MULTIPLE_QUERY_PATH);
  }

  @Override
  public Future<JobExecutionCollection> getJobExecutionsWithoutParentMultiple(String query, int offset, int limit, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      LOGGER.info("JobExecutionDaoImpl.getJobExecutionsWithoutParentMultiple.start");
      StringBuilder cqlQuery = new StringBuilder("subordinationType=\"\" NOT subordinationType=").append(PARENT_MULTIPLE);
      if (StringUtils.isNotEmpty(query)) {
        cqlQuery.append(" and ").append(query);
      }
      SqlSelect sqlSelect = new CQL2PgJSON(TABLE_NAME + ".jsonb").toSql(cqlQuery.toString());
      String preparedQuery = prepareQueryGetJobWithoutParentMultiple(sqlSelect, limit, offset, convertToPsqlStandard(tenantId));
      PostgresClient client = pgClientFactory.createInstance(tenantId);

      Field privateStringField = PostgresClient.class.getDeclaredField("client");
      privateStringField.setAccessible(true);

      //LOGGER.info(client.getConnectionConfig().encodePrettily());

      String db = client.getConnectionConfig().getString("database");
      String host = client.getConnectionConfig().getString("host");
      String port = client.getConnectionConfig().getString("port");
      String username = client.getConnectionConfig().getString("username");
      String password = client.getConnectionConfig().getString("password");

      PgConnectOptions co = new PgConnectOptions().setPort(Integer.parseInt(port)).setHost(host)
        .setDatabase(db).setUser(username).setPassword(password);

      PgPool pgPool = PgPool.pool(pgClientFactory.vertx, co, new PoolOptions().setMaxSize(5));
      privateStringField.set(client, pgPool);

      client.select(preparedQuery, promise);
      LOGGER.info("JobExecutionDaoImpl.getJobExecutionsWithoutParentMultiple.finish");
    } catch (Exception e) {
      LOGGER.error("Error while getting Logs", e);
      promise.fail(e);
    }
    return promise.future().map(this::mapResultSetToJobExecutionCollection);
  }

  @Override
  public Future<JobExecutionCollection> getChildrenJobExecutionsByParentId(String parentId, String query, int offset, int limit, String tenantId) {
    Promise<Results<JobExecution>> promise = Promise.promise();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cqlWrapper = getCQLWrapper(TABLE_NAME, query, limit, offset);
      cqlWrapper.addWrapper(getCQLWrapper(TABLE_NAME, "parentJobId=" + parentId));
      cqlWrapper.addWrapper(getCQLWrapper(TABLE_NAME, "subordinationType=" + CHILD));
      pgClientFactory.createInstance(tenantId).get(TABLE_NAME, JobExecution.class, fieldList, cqlWrapper, true, false, promise);
    } catch (Exception e) {
      LOGGER.error("Error getting jobExecutions by parent id", e);
      promise.fail(e);
    }
    return promise.future().map(results -> new JobExecutionCollection()
      .withJobExecutions(results.getResults())
      .withTotalRecords(results.getResultInfo().getTotalRecords()));
  }

  @Override
  public Future<Optional<JobExecution>> getJobExecutionById(String id, String tenantId) {
    Promise<Results<JobExecution>> promise = Promise.promise();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, id);
      pgClientFactory.createInstance(tenantId).get(TABLE_NAME, JobExecution.class, new Criterion(idCrit), true, false, promise);
    } catch (Exception e) {
      LOGGER.error("Error getting jobExecution by id", e);
      promise.fail(e);
    }
    return promise.future()
      .map(Results::getResults)
      .map(jobExecutions -> jobExecutions.isEmpty() ? Optional.empty() : Optional.of(jobExecutions.get(0)));
  }

  @Override
  public Future<String> save(JobExecution jobExecution, String tenantId) {
    Promise<String> promise = Promise.promise();
    String preparedQuery = String.format(GET_JOB_EXECUTION_HR_ID, PostgresClient.convertToPsqlStandard(tenantId));
    pgClientFactory.createInstance(tenantId).select(preparedQuery, getHrIdAr -> {
      if (getHrIdAr.succeeded() && getHrIdAr.result().iterator().hasNext()) {
        jobExecution.setHrId(getHrIdAr.result().iterator().next().getInteger(0));
        pgClientFactory.createInstance(tenantId).save(TABLE_NAME, jobExecution.getId(), jobExecution, promise);
      } else {
        promise.fail(getHrIdAr.cause());
      }
    });
    return promise.future();
  }

  @Override
  public Future<JobExecution> updateJobExecution(JobExecution jobExecution, String tenantId) {
    Promise<JobExecution> promise = Promise.promise();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, jobExecution.getId());
      pgClientFactory.createInstance(tenantId).update(TABLE_NAME, jobExecution, new Criterion(idCrit), true, updateResult -> {
        if (updateResult.failed()) {
          LOGGER.error("Could not update jobExecution with id {}", jobExecution.getId(), updateResult.cause());
          promise.fail(updateResult.cause());
        } else if (updateResult.result().rowCount() != 1) {
          String errorMessage = String.format("JobExecution with id '%s' was not found", jobExecution.getId());
          LOGGER.error(errorMessage);
          promise.fail(new NotFoundException(errorMessage));
        } else {
          promise.complete(jobExecution);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Error updating jobExecution", e);
      promise.fail(e);
    }
    return promise.future();
  }

  @Override
  public Future<JobExecution> updateBlocking(String jobExecutionId, JobExecutionMutator mutator, String tenantId) {
    Promise<JobExecution> promise = Promise.promise();
    String rollbackMessage = "Rollback transaction. Error during jobExecution update. jobExecutionId" + jobExecutionId;
    Promise<SQLConnection> connection = Promise.promise();
    Promise<JobExecution> jobExecutionPromise = Promise.promise();
    Future.succeededFuture()
      .compose(v -> {
        pgClientFactory.createInstance(tenantId).startTx(connection);
        return connection.future();
      }).compose(v -> {
      StringBuilder selectJobExecutionQuery = new StringBuilder("SELECT jsonb FROM ")
        .append(PostgresClient.convertToPsqlStandard(tenantId))
        .append(".")
        .append(TABLE_NAME)
        .append(" WHERE id ='")
        .append(jobExecutionId).append("' LIMIT 1 FOR UPDATE;");
      Promise<RowSet<Row>> selectResult = Promise.promise();
      pgClientFactory.createInstance(tenantId).execute(connection.future(), selectJobExecutionQuery.toString(), selectResult);
      return selectResult.future();
    }).compose(selectResult -> {
      if (selectResult.rowCount() != 1) {
        throw new NotFoundException(rollbackMessage);
      }
      Criteria idCrit = constructCriteria(ID_FIELD, jobExecutionId);
      Promise<Results<JobExecution>> jobExecResult = Promise.promise();
      pgClientFactory.createInstance(tenantId).get(connection.future(), TABLE_NAME, JobExecution.class, new Criterion(idCrit), false, true, jobExecResult);
      return jobExecResult.future();
    }).compose(jobExecResult -> {
      if (jobExecResult.getResults().size() != 1) {
        throw new NotFoundException(rollbackMessage);
      }
      JobExecution jobExecution = jobExecResult.getResults().get(0);
      mutator.mutate(jobExecution).onComplete(jobExecutionPromise);
      return jobExecutionPromise.future();
    }).compose(jobExecution -> {
      CQLWrapper filter;
      try {
        filter = getCQLWrapper(TABLE_NAME, "id==" + jobExecution.getId());
      } catch (FieldException e) {
        throw new RuntimeException(e);
      }
      Promise<RowSet<Row>> updateHandler = Promise.promise();
      pgClientFactory.createInstance(tenantId).update(connection.future(), TABLE_NAME, jobExecution, filter, true, updateHandler);
      return updateHandler.future();
    }).compose(updateHandler -> {
      if (updateHandler.rowCount() != 1) {
        throw new NotFoundException(rollbackMessage);
      }
      Promise<Void> endTxFuture = Promise.promise();
      pgClientFactory.createInstance(tenantId).endTx(connection.future(), endTxFuture);
      return endTxFuture.future();
    }).onComplete(v -> {
      if (v.failed()) {
        pgClientFactory.createInstance(tenantId).rollbackTx(connection.future(), rollback -> promise.fail(v.cause()));
        return;
      }
      promise.complete(jobExecutionPromise.future().result());
    });
    return promise.future();
  }

  @Override
  public Future<Boolean> deleteJobExecutionById(String jobExecutionId, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    pgClientFactory.createInstance(tenantId).delete(TABLE_NAME, jobExecutionId, promise);
    return promise.future().map(updateResult -> updateResult.rowCount() == 1);
  }

  private String prepareQueryGetJobWithoutParentMultiple(SqlSelect sqlSelect, int limit, int offset, String schemaName) {
    String whereClause = String.format("WHERE %s ", sqlSelect.getWhere());
    String orderBy = sqlSelect.getOrderBy().isEmpty() ? StringUtils.EMPTY : String.format("ORDER BY %s", sqlSelect.getOrderBy());
    return String.format(getJobsWithoutParentMultipleSql, schemaName, whereClause, schemaName, schemaName, whereClause, orderBy, limit, offset);
  }

  private JobExecutionCollection mapResultSetToJobExecutionCollection(RowSet<Row> resultSet) {
    int totalRecords = resultSet.rowCount() != 0 ? resultSet.iterator().next().getInteger(TOTAL_ROWS_COLUMN) : 0;
    List<JobExecution> jobExecutions = new ArrayList<>();
    resultSet.forEach(row -> jobExecutions.add(mapJsonToJobExecution(row.getValue(JSONB_COLUMN).toString())));

    return new JobExecutionCollection()
      .withJobExecutions(jobExecutions)
      .withTotalRecords(totalRecords);
  }

  private JobExecution mapJsonToJobExecution(String jsonAsString) {
    try {
      return new ObjectMapper().readValue(jsonAsString, JobExecution.class);
    } catch (IOException e) {
      LOGGER.error("Error while mapping json to jobExecution", e);
      throw new RuntimeJsonMappingException(e.getMessage());
    }
  }

}
