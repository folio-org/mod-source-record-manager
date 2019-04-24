package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import org.folio.dao.util.JobExecutionMutator;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.z3950.zing.cql.cql2pgjson.FieldException;

import javax.ws.rs.NotFoundException;
import java.util.Optional;

import static org.folio.dataimport.util.DaoUtil.constructCriteria;
import static org.folio.dataimport.util.DaoUtil.getCQLWrapper;
import static org.folio.rest.jaxrs.model.JobExecution.Status.COMMITTED;
import static org.folio.rest.jaxrs.model.JobExecution.Status.DISCARDED;
import static org.folio.rest.jaxrs.model.JobExecution.Status.ERROR;
import static org.folio.rest.jaxrs.model.JobExecution.SubordinationType.PARENT_MULTIPLE;

/**
 * Implementation for the JobExecutionDao, works with PostgresClient to access data.
 *
 * @see JobExecution
 * @see JobExecutionDao
 * @see org.folio.rest.persist.PostgresClient
 */
@Repository
public class JobExecutionDaoImpl implements JobExecutionDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutionDaoImpl.class);

  private static final String TABLE_NAME = "job_executions";
  private static final String ID_FIELD = "'id'";

  @Autowired
  private PostgresClientFactory pgClientFactory;

  @Override
  public Future<JobExecutionCollection> getJobExecutionsWithoutParentMultiple(String query, int offset, int limit, String tenantId) {
    Future<Results<JobExecution>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cqlWrapper = getCQLWrapper(TABLE_NAME, "subordinationType=\"\" NOT subordinationType=" + PARENT_MULTIPLE, limit, offset);
      cqlWrapper.addWrapper(new CQLWrapper(cqlWrapper.getField(), "status=\"\" NOT status=" + DISCARDED));
      cqlWrapper.addWrapper(new CQLWrapper(cqlWrapper.getField(), query));
      pgClientFactory.createInstance(tenantId).get(TABLE_NAME, JobExecution.class, fieldList, cqlWrapper, true, false, future.completer());
    } catch (Exception e) {
      LOGGER.error("Error while getting JobExecutions", e);
      future.fail(e);
    }
    return future.map(results -> new JobExecutionCollection()
      .withJobExecutions(results.getResults())
      .withTotalRecords(results.getResultInfo().getTotalRecords()));
  }

  @Override
  public Future<JobExecutionCollection> getLogsWithoutMultipleParent(String query, int offset, int limit, String tenantId) {
    Future<Results<JobExecution>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cqlWrapper = getCQLWrapper(TABLE_NAME, query, limit, offset);
      cqlWrapper.addWrapper(new CQLWrapper(cqlWrapper.getField(), "status any \"" + COMMITTED + " " + ERROR + " \""));
      String excludeParentMultipleAndSortQuery = "subordinationType=\"\" NOT subordinationType=" + PARENT_MULTIPLE + " sortBy completedDate/sort.descending";
      cqlWrapper.addWrapper(new CQLWrapper(cqlWrapper.getField(), excludeParentMultipleAndSortQuery));
      pgClientFactory.createInstance(tenantId).get(TABLE_NAME, JobExecution.class, fieldList, cqlWrapper, true, false, future.completer());
    } catch (Exception e) {
      LOGGER.error("Error while getting Logs", e);
      future.fail(e);
    }
    return future.map(results -> new JobExecutionCollection()
      .withJobExecutions(results.getResults())
      .withTotalRecords(results.getResultInfo().getTotalRecords()));
  }

  @Override
  public Future<JobExecutionCollection> getChildrenJobExecutionsByParentId(String parentId, String query, int offset, int limit, String tenantId) {
    Future<Results<JobExecution>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cqlWrapper = getCQLWrapper(TABLE_NAME, query, limit, offset);
      cqlWrapper.addWrapper(new CQLWrapper(cqlWrapper.getField(), "parentJobId=" + parentId));
      cqlWrapper.addWrapper(new CQLWrapper(cqlWrapper.getField(), "subordinationType=" + JobExecution.SubordinationType.CHILD.name()));
      pgClientFactory.createInstance(tenantId).get(TABLE_NAME, JobExecution.class, fieldList, cqlWrapper, true, false, future.completer());
    } catch (Exception e) {
      LOGGER.error("Error getting jobExecutions by parent id", e);
      future.fail(e);
    }
    return future.map(results -> new JobExecutionCollection()
      .withJobExecutions(results.getResults())
      .withTotalRecords(results.getResultInfo().getTotalRecords()));
  }

  @Override
  public Future<Optional<JobExecution>> getJobExecutionById(String id, String tenantId) {
    Future<Results<JobExecution>> future = Future.future();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, id);
      pgClientFactory.createInstance(tenantId).get(TABLE_NAME, JobExecution.class, new Criterion(idCrit), true, false, future.completer());
    } catch (Exception e) {
      LOGGER.error("Error getting jobExecution by id", e);
      future.fail(e);
    }
    return future
      .map(Results::getResults)
      .map(jobExecutions -> jobExecutions.isEmpty() ? Optional.empty() : Optional.of(jobExecutions.get(0)));
  }

  @Override
  public Future<String> save(JobExecution jobExecution, String tenantId) {
    Future<String> future = Future.future();
    pgClientFactory.createInstance(tenantId).save(TABLE_NAME, jobExecution.getId(), jobExecution, future.completer());
    return future;
  }

  @Override
  public Future<JobExecution> updateJobExecution(JobExecution jobExecution, String tenantId) {
    Future<JobExecution> future = Future.future();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, jobExecution.getId());
      pgClientFactory.createInstance(tenantId).update(TABLE_NAME, jobExecution, new Criterion(idCrit), true, updateResult -> {
        if (updateResult.failed()) {
          LOGGER.error("Could not update jobExecution with id {}", jobExecution.getId(), updateResult.cause());
          future.fail(updateResult.cause());
        } else if (updateResult.result().getUpdated() != 1) {
          String errorMessage = String.format("JobExecution with id '%s' was not found", jobExecution.getId());
          LOGGER.error(errorMessage);
          future.fail(new NotFoundException(errorMessage));
        } else {
          future.complete(jobExecution);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Error updating jobExecution", e);
      future.fail(e);
    }
    return future;
  }

  @Override
  public Future<JobExecution> updateBlocking(String jobExecutionId, JobExecutionMutator mutator, String tenantId) {
    Future<JobExecution> future = Future.future();
    String rollbackMessage = "Rollback transaction. Error during jobExecution update. jobExecutionId" + jobExecutionId; //NOSONAR
    Future<SQLConnection> tx = Future.future(); //NOSONAR
    Future<JobExecution> jobExecutionFuture = Future.future(); //NOSONAR
    Future.succeededFuture()
      .compose(v -> {
        pgClientFactory.createInstance(tenantId).startTx(tx.completer());
        return tx;
      }).compose(v -> {
      StringBuilder selectJobExecutionQuery = new StringBuilder("SELECT jsonb FROM ") //NOSONAR
        .append(PostgresClient.convertToPsqlStandard(tenantId))
        .append(".")
        .append(TABLE_NAME)
        .append(" WHERE _id ='")
        .append(jobExecutionId).append("' LIMIT 1 FOR UPDATE;");
      Future<UpdateResult> selectResult = Future.future(); //NOSONAR
      pgClientFactory.createInstance(tenantId).execute(tx, selectJobExecutionQuery.toString(), selectResult);
      return selectResult;
    }).compose(selectResult -> {
      if (selectResult.getUpdated() != 1) {
        throw new NotFoundException(rollbackMessage);
      }
      Criteria idCrit = constructCriteria(ID_FIELD, jobExecutionId); //NOSONAR
      Future<Results<JobExecution>> jobExecResult = Future.future(); //NOSONAR
      pgClientFactory.createInstance(tenantId).get(tx, TABLE_NAME, JobExecution.class, new Criterion(idCrit), false, true, jobExecResult);
      return jobExecResult;
    }).compose(jobExecResult -> {
      if (jobExecResult.getResults().size() != 1) {
        throw new NotFoundException(rollbackMessage);
      }
      JobExecution jobExecution = jobExecResult.getResults().get(0); //NOSONAR
      mutator.mutate(jobExecution).setHandler(jobExecutionFuture);
      return jobExecutionFuture;
    }).compose(jobExecution -> {
      CQLWrapper filter; //NOSONAR
      try {
        filter = getCQLWrapper(TABLE_NAME, "id==" + jobExecution.getId());
      } catch (FieldException e) {
        throw new RuntimeException(e);
      }
      Future<UpdateResult> updateHandler = Future.future(); //NOSONAR
      pgClientFactory.createInstance(tenantId).update(tx, TABLE_NAME, jobExecution, filter, true, updateHandler);
      return updateHandler;
    }).compose(updateHandler -> {
      if (updateHandler.getUpdated() != 1) {
        throw new NotFoundException(rollbackMessage);
      }
      Future<Void> endTxFuture = Future.future(); //NOSONAR
      pgClientFactory.createInstance(tenantId).endTx(tx, endTxFuture);
      return endTxFuture;
    }).setHandler(v -> {
      if (v.failed()) {
        pgClientFactory.createInstance(tenantId).rollbackTx(tx, rollback -> future.fail(v.cause()));
        return;
      }
      future.complete(jobExecutionFuture.result());
    });
    return future;
  }

}
