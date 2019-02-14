package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.UpdateResult;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;

import javax.ws.rs.NotFoundException;
import java.util.List;
import java.util.Optional;

import static org.folio.dataimport.util.DaoUtil.constructCriteria;
import static org.folio.dataimport.util.DaoUtil.getCQLWrapper;

/**
 * Implementation for the JobExecutionSourceChunkDao, works with PostgresClient to access data.
 *
 * @see JobExecutionSourceChunk
 * @see JobExecutionSourceChunkDao
 * @see org.folio.rest.persist.PostgresClient
 */
public class JobExecutionSourceChunkDaoImpl implements JobExecutionSourceChunkDao {

  public static final Logger LOGGER = LoggerFactory.getLogger(JobExecutionSourceChunkDaoImpl.class);

  private static final String TABLE_NAME = "job_execution_source_chunks";
  private static final String ID_FIELD = "'id'";
  private PostgresClient pgClient;

  public JobExecutionSourceChunkDaoImpl(Vertx vertx, String tenantId) {
    this.pgClient = PostgresClient.getInstance(vertx, tenantId);
  }

  @Override
  public Future<String> save(JobExecutionSourceChunk jobExecutionChunk) {
    Future<String> future = Future.future();
    pgClient.save(TABLE_NAME, jobExecutionChunk.getId(), jobExecutionChunk, future.completer());
    return future;
  }

  @Override
  public Future<List<JobExecutionSourceChunk>> get(String query, int offset, int limit) {
    Future<Results<JobExecutionSourceChunk>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cql = getCQLWrapper(TABLE_NAME, query, limit, offset);
      pgClient.get(TABLE_NAME, JobExecutionSourceChunk.class, fieldList, cql, true, false, future.completer());
    } catch (Exception e) {
      LOGGER.error("Error while searching for JobExecutionSourceChunks", e);
      future.fail(e);
    }
    return future.map(Results::getResults);
  }

  @Override
  public Future<Optional<JobExecutionSourceChunk>> getById(String id) {
    Future<Results<JobExecutionSourceChunk>> future = Future.future();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, id);
      pgClient.get(TABLE_NAME, JobExecutionSourceChunk.class, new Criterion(idCrit), true, false, future.completer());
    } catch (Exception e) {
      LOGGER.error("Error querying JobExecutionSourceChunk by id {}", id, e);
      future.fail(e);
    }
    return future
      .map(Results::getResults)
      .map(jobExecutionSourceChunks -> jobExecutionSourceChunks.isEmpty() ? Optional.empty() : Optional.of(jobExecutionSourceChunks.get(0)));
  }

  @Override
  public Future<JobExecutionSourceChunk> update(JobExecutionSourceChunk jobExecutionChunk) {
    Future<JobExecutionSourceChunk> future = Future.future();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, jobExecutionChunk.getId());
      pgClient.update(TABLE_NAME, jobExecutionChunk, new Criterion(idCrit), true, updateResult -> {
        if (updateResult.failed()) {
          LOGGER.error("Could not update jobExecutionSourceChunk with id {}", jobExecutionChunk.getId(), updateResult.cause());
          future.fail(updateResult.cause());
        } else if (updateResult.result().getUpdated() != 1) {
          String errorMessage = String.format("JobExecutionSourceChunk with id '%s' was not found", jobExecutionChunk.getId());
          LOGGER.error(errorMessage);
          future.fail(new NotFoundException(errorMessage));
        } else {
          future.complete(jobExecutionChunk);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Error updating jobExecutionSourceChunk", e);
      future.fail(e);
    }
    return future;
  }

  @Override
  public Future<Boolean> delete(String id) {
    Future<UpdateResult> future = Future.future();
    pgClient.delete(TABLE_NAME, id, future.completer());
    return future.map(updateResult -> updateResult.getUpdated() == 1);
  }
}
