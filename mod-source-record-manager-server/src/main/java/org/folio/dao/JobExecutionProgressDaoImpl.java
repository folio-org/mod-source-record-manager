package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import org.folio.dao.util.JobExecutionProgressMutator;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.Progress;
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
  private static final String ID_FIELD = "'jobExecutionId'";

  @Autowired
  private PostgresClientFactory pgClientFactory;

  @Override
  public Future<String> save(Progress progress, String tenantId) {
    Future<String> future = Future.future();
    pgClientFactory.createInstance(tenantId).save(TABLE_NAME, progress.getJobExecutionId(), progress, future.completer());
    return future;
  }

  @Override
  public Future<Progress> updateBlocking(String jobExecutionId, JobExecutionProgressMutator mutator, String tenantId) {
    Future<Progress> future = Future.future();
    String rollbackMessage = "Rollback transaction. Error during Progress update. jobExecutionId" + jobExecutionId; //NOSONAR
    Future<SQLConnection> tx = Future.future(); //NOSONAR
    Future<Progress> progressFuture = Future.future(); //NOSONAR
    Future.succeededFuture()
      .compose(v -> {
        pgClientFactory.createInstance(tenantId).startTx(tx.completer());
        return tx;
      }).compose(v -> {
      StringBuilder selectProgressQuery = new StringBuilder("SELECT jsonb FROM ") //NOSONAR
        .append(PostgresClient.convertToPsqlStandard(tenantId))
        .append(".")
        .append(TABLE_NAME)
        .append(" WHERE _id ='")
        .append(jobExecutionId).append("' LIMIT 1 FOR UPDATE;");
      Future<UpdateResult> selectResult = Future.future(); //NOSONAR
      pgClientFactory.createInstance(tenantId).execute(tx, selectProgressQuery.toString(), selectResult);
      return selectResult;
    }).compose(selectResult -> {
      if (selectResult.getUpdated() != 1) {
        throw new NotFoundException(rollbackMessage);
      }
      Criteria idCrit = constructCriteria(ID_FIELD, jobExecutionId); //NOSONAR
      Future<Results<Progress>> progressResult = Future.future(); //NOSONAR
      pgClientFactory.createInstance(tenantId).get(tx, TABLE_NAME, Progress.class, new Criterion(idCrit), false, true, progressResult);
      return progressResult;
    }).compose(progressResult -> {
      if (progressResult.getResults().size() != 1) {
        throw new NotFoundException(rollbackMessage);
      }
      Progress progress = progressResult.getResults().get(0); //NOSONAR
      mutator.mutate(progress).setHandler(progressFuture);
      return progressFuture;
    }).compose(progress -> {
      CQLWrapper filter; //NOSONAR
      try {
        filter = getCQLWrapper(TABLE_NAME, "id==" + progress.getJobExecutionId());
      } catch (FieldException e) {
        throw new RuntimeException(e);
      }
      Future<UpdateResult> updateHandler = Future.future(); //NOSONAR
      pgClientFactory.createInstance(tenantId).update(tx, TABLE_NAME, progress, filter, true, updateHandler);
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
      future.complete(progressFuture.result());
    });
    return future;
  }

  @Override
  public Future<Optional<Progress>> getProgressByJobExecutionId(String jobExecutionId, String tenantId) {
    Future<Results<Progress>> future = Future.future();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, jobExecutionId);
      pgClientFactory.createInstance(tenantId).get(TABLE_NAME, Progress.class, new Criterion(idCrit), true, false, future.completer());
    } catch (Exception e) {
      LOGGER.error("Error querying Progress by id {}", jobExecutionId, e);
      future.fail(e);
    }
    return future
      .map(Results::getResults)
      .map(progress -> progress.isEmpty() ? Optional.empty() : Optional.of(progress.get(0)));
  }
}
