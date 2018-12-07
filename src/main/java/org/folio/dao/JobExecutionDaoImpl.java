package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;

import javax.ws.rs.NotFoundException;
import java.util.List;
import java.util.Optional;

import static org.folio.dao.util.DaoUtil.constructCriteria;
import static org.folio.dao.util.DaoUtil.getCQLWrapper;
import static org.folio.rest.jaxrs.model.JobExecution.Status.COMMITTED;
import static org.folio.rest.jaxrs.model.JobExecution.Status.ERROR;
import static org.folio.rest.jaxrs.model.JobExecution.SubordinationType.PARENT_MULTIPLE;

/**
 * Implementation for the JobExecutionDao, works with PostgresClient to access data.
 *
 * @see JobExecution
 * @see JobExecutionDao
 * @see org.folio.rest.persist.PostgresClient
 */
public class JobExecutionDaoImpl implements JobExecutionDao {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobExecutionDaoImpl.class);

  private static final String TABLE_NAME = "job_executions";
  private static final String ID_FIELD = "'id'";
  private static final String PARENT_ID = "'parentJobId'";
  private PostgresClient pgClient;

  public JobExecutionDaoImpl(Vertx vertx, String tenantId) {
    this.pgClient = PostgresClient.getInstance(vertx, tenantId);
  }

  @Override
  public Future<JobExecutionCollection> getJobExecutionsWithoutParentMultiple(String query, int offset, int limit) {
    Future<Results<JobExecution>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cqlWrapper = getCQLWrapper(TABLE_NAME, query, limit, offset);
      cqlWrapper.addWrapper(new CQLWrapper(cqlWrapper.getField(), "subordinationType=\"\" NOT subordinationType=" + PARENT_MULTIPLE));
      pgClient.get(TABLE_NAME, JobExecution.class, fieldList, cqlWrapper, true, false, future.completer());
    } catch (Exception e) {
      LOGGER.error("Error while getting JobExecutions", e);
      future.fail(e);
    }
    return future.map(results -> new JobExecutionCollection()
      .withJobExecutions(results.getResults())
      .withTotalRecords(results.getResultInfo().getTotalRecords()));
  }

  @Override
  public Future<JobExecutionCollection> getLogsWithoutMultipleParent(String query, int offset, int limit) {
    Future<Results<JobExecution>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cqlWrapper = getCQLWrapper(TABLE_NAME, query, limit, offset);
      cqlWrapper.addWrapper(new CQLWrapper(cqlWrapper.getField(), "status any \"" + COMMITTED + " " + ERROR + " \""));
      String excludeParentMultipleAndSortQuery = "subordinationType=\"\" NOT subordinationType=" + PARENT_MULTIPLE + " sortBy completedDate/sort.descending";
      cqlWrapper.addWrapper(new CQLWrapper(cqlWrapper.getField(), excludeParentMultipleAndSortQuery));
      pgClient.get(TABLE_NAME, JobExecution.class, fieldList, cqlWrapper, true, false, future.completer());
    } catch (Exception e) {
      LOGGER.error("Error while getting Logs", e);
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

  @Override
  public Future<JobExecution> updateJobExecution(JobExecution jobExecution) {
    Future<JobExecution> future = Future.future();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, jobExecution.getId());
      pgClient.update(TABLE_NAME, jobExecution, new Criterion(idCrit), true, updateResult -> {
        if (updateResult.failed()) {
          LOGGER.error(String.format("Could not update jobExecution with id '%s'", jobExecution.getId()), updateResult.cause());
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
  public Future<List<JobExecution>> getJobExecutionsByParentId(String parentId) {
    Future<Results<JobExecution>> future = Future.future();
    try {
      Criteria idCrit = constructCriteria(PARENT_ID, parentId);
      pgClient.get(TABLE_NAME, JobExecution.class, new Criterion(idCrit), true, false, future.completer());
    } catch (Exception e) {
      LOGGER.error("Error getting jobExecutions by parent id", e);
      future.fail(e);
    }
    return future.map(Results::getResults);
  }

  @Override
  public Future<Optional<JobExecution>> getJobExecutionById(String id) {
    Future<Results<JobExecution>> future = Future.future();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, id);
      pgClient.get(TABLE_NAME, JobExecution.class, new Criterion(idCrit), true, false, future.completer());
    } catch (Exception e) {
      LOGGER.error("Error getting jobExecution by id", e);
      future.fail(e);
    }
    return future
      .map(Results::getResults)
      .map(jobExecutions -> jobExecutions.isEmpty() ? Optional.empty() : Optional.of(jobExecutions.get(0)));
  }

}
