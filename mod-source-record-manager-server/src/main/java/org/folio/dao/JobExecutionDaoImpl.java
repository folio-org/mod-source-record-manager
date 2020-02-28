package org.folio.dao;

import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
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
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;
import org.folio.rest.tools.utils.ObjectMapperTool;
import org.folio.util.ResourceUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import javax.ws.rs.NotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

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
@SuppressWarnings("squid:CallToDeprecatedMethod")
public class JobExecutionDaoImpl implements JobExecutionDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutionDaoImpl.class);

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
    try {
      getJobsWithoutParentMultipleSql = ResourceUtil.asString(GET_JOBS_WITHOUT_PARENT_MULTIPLE_QUERY_PATH);
    } catch (UncheckedIOException e) {
      LOGGER.error("Error while reading query from file {}", GET_JOBS_WITHOUT_PARENT_MULTIPLE_QUERY_PATH, e);
      throw e;
    }
  }

  @Override
  public Future<JobExecutionCollection> getJobExecutionsWithoutParentMultiple(String query, int offset, int limit, String tenantId) {
    Promise<ResultSet> promise = Promise.promise();
    try {
      StringBuilder cqlQuery = new StringBuilder("subordinationType=\"\" NOT subordinationType=").append(PARENT_MULTIPLE);
      if (StringUtils.isNotEmpty(query)) {
        cqlQuery.append(" and ").append(query);
      }
      SqlSelect sqlSelect = new CQL2PgJSON(TABLE_NAME + ".jsonb").toSql(cqlQuery.toString());
      String preparedQuery = prepareQueryGetJobWithoutParentMultiple(sqlSelect, limit, offset, convertToPsqlStandard(tenantId));
      pgClientFactory.createInstance(tenantId).select(preparedQuery, promise);
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
      if (getHrIdAr.succeeded() && getHrIdAr.result().getResults().get(0) != null) {
        jobExecution.setHrId(getHrIdAr.result().getResults().get(0).getInteger(0));
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
        } else if (updateResult.result().getUpdated() != 1) {
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
    Future<JobExecution> jobExecutionFuture = Future.future();
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
      Promise<UpdateResult> selectResult = Promise.promise();
      pgClientFactory.createInstance(tenantId).execute(connection.future(), selectJobExecutionQuery.toString(), selectResult);
      return selectResult.future();
    }).compose(selectResult -> {
      if (selectResult.getUpdated() != 1) {
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
      mutator.mutate(jobExecution).setHandler(jobExecutionFuture);
      return jobExecutionFuture;
    }).compose(jobExecution -> {
      CQLWrapper filter;
      try {
        filter = getCQLWrapper(TABLE_NAME, "id==" + jobExecution.getId());
      } catch (FieldException e) {
        throw new RuntimeException(e);
      }
      Promise<UpdateResult> updateHandler = Promise.promise();
      pgClientFactory.createInstance(tenantId).update(connection.future(), TABLE_NAME, jobExecution, filter, true, updateHandler);
      return updateHandler.future();
    }).compose(updateHandler -> {
      if (updateHandler.getUpdated() != 1) {
        throw new NotFoundException(rollbackMessage);
      }
      Promise<Void> endTxFuture = Promise.promise();
      pgClientFactory.createInstance(tenantId).endTx(connection.future(), endTxFuture);
      return endTxFuture.future();
    }).setHandler(v -> {
      if (v.failed()) {
        pgClientFactory.createInstance(tenantId).rollbackTx(connection.future(), rollback -> promise.fail(v.cause()));
        return;
      }
      promise.complete(jobExecutionFuture.result());
    });
    return promise.future();
  }

  @Override
  public Future<Boolean> deleteJobExecutionById(String jobExecutionId, String tenantId) {
    Promise<UpdateResult> promise = Promise.promise();
    pgClientFactory.createInstance(tenantId).delete(TABLE_NAME, jobExecutionId, promise);
    return promise.future().map(updateResult -> updateResult.getUpdated() == 1);
  }

  private String prepareQueryGetJobWithoutParentMultiple(SqlSelect sqlSelect, int limit, int offset, String schemaName) {
    String whereClause = String.format("WHERE %s ", sqlSelect.getWhere());
    String orderBy = sqlSelect.getOrderBy().isEmpty() ? StringUtils.EMPTY : String.format("ORDER BY %s", sqlSelect.getOrderBy());
    return String.format(getJobsWithoutParentMultipleSql, schemaName, whereClause, schemaName, schemaName, whereClause, orderBy, limit, offset);
  }

  private JobExecutionCollection mapResultSetToJobExecutionCollection(ResultSet resultSet) {
    int totalRecords = resultSet.getNumRows() != 0 ? resultSet.getRows(false).get(0).getInteger(TOTAL_ROWS_COLUMN) : 0;
    List<JobExecution> jobExecutions = resultSet.getRows().stream()
      .map(row -> mapJsonToJobExecution(row.getString(JSONB_COLUMN)))
      .collect(Collectors.toList());

    return new JobExecutionCollection()
      .withJobExecutions(jobExecutions)
      .withTotalRecords(totalRecords);
  }

  private JobExecution mapJsonToJobExecution(String jsonString) {
    try {
      return ObjectMapperTool.getMapper().readValue(jsonString, JobExecution.class);
    } catch (IOException e) {
      LOGGER.error("Error while mapping json to jobExecution", e);
      throw new RuntimeJsonMappingException(e.getMessage());
    }
  }

}
