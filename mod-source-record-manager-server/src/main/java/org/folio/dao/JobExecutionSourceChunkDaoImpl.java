package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Promise;

import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Tuple;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.StringUtils;


import java.util.Optional;
import java.util.UUID;

import javax.ws.rs.NotFoundException;

import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.interfaces.Results;
import org.springframework.stereotype.Repository;

import static org.folio.dataimport.util.DaoUtil.constructCriteria;
import static java.lang.String.format;
import static org.folio.rest.persist.PostgresClient.convertToPsqlStandard;

/**
 * Implementation for the JobExecutionSourceChunkDao, works with PostgresClient to access data.
 *
 * @see JobExecutionSourceChunk
 * @see JobExecutionSourceChunkDao
 * @see org.folio.rest.persist.PostgresClient
 */
@Log4j2
@Repository
@RequiredArgsConstructor
public class JobExecutionSourceChunkDaoImpl implements JobExecutionSourceChunkDao {

  private static final String TABLE_NAME = "job_execution_source_chunks";

  private static final String ID_FIELD = "id";
  private static final String JOB_EXECUTION_ID_FIELD = "jobexecutionid";

  private static final String IS_PROCESSING_COMPLETED_QUERY = "SELECT is_processing_completed('%s');";
  private static final String INSERT_QUERY = "INSERT INTO %s.%s (id, jsonb, jobExecutionId) VALUES ($1, $2, $3)";

  private final PostgresClientFactory pgClientFactory;

  @Override
  public Future<String> save(JobExecutionSourceChunk jobExecutionChunk, String tenantId) {
    try {
      log.trace("save:: Saving jobExecutionSourceChunk {} for tenant {}", jobExecutionChunk.getId(), tenantId);
      String query = format(INSERT_QUERY, convertToPsqlStandard(tenantId), TABLE_NAME);
      Tuple queryParams = Tuple.of(
        StringUtils.defaultIfEmpty(jobExecutionChunk.getId(), /* generate UUID for the empty last chunk */ UUID.randomUUID().toString()),
        JsonObject.mapFrom(jobExecutionChunk),
        jobExecutionChunk.getJobExecutionId());
      return pgClientFactory.createInstance(tenantId).execute(query, queryParams)
        .map(jobExecutionChunk.getId());
    } catch (Exception e) {
      log.warn("save:: Failed to save JobExecutionSourceChunk with id: {}", jobExecutionChunk.getId(), e);
      return Future.failedFuture(e);
    }
  }

    @Override
  public Future<Optional<JobExecutionSourceChunk>> getById(String id, String tenantId) {
    try {
      if (StringUtils.isBlank(id)) {
        log.warn("getById:: Can't retrieve JobExecutionSourceChunk by empty id.");
        return Future.succeededFuture(Optional.empty());
      }
      Criteria idCrit = constructCriteria(ID_FIELD, id).setJSONB(false);
      return pgClientFactory.createInstance(tenantId)
        .get(TABLE_NAME, JobExecutionSourceChunk.class, new Criterion(idCrit), true)
        .map(Results::getResults)
        .map(jobExecutionSourceChunks -> jobExecutionSourceChunks.isEmpty()
          ? Optional.empty() : Optional.of(jobExecutionSourceChunks.getFirst()));
    } catch (Exception e) {
      log.warn("getById:: Error querying JobExecutionSourceChunk by id {}", id, e);
      return Future.failedFuture(e);
    }
  }

  @Override
  public Future<JobExecutionSourceChunk> update(JobExecutionSourceChunk jobExecutionChunk, String tenantId) {
    Promise<JobExecutionSourceChunk> promise = Promise.promise();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, jobExecutionChunk.getId()).setJSONB(false);
      pgClientFactory.createInstance(tenantId)
        .update(TABLE_NAME, jobExecutionChunk, new Criterion(idCrit), true, updateResult -> {
          if (updateResult.failed()) {
            log.warn("update:: Could not update jobExecutionSourceChunk with id {}", jobExecutionChunk.getId(), updateResult.cause());
            promise.fail(updateResult.cause());
          } else if (updateResult.result().rowCount() != 1) {
            String errorMessage = String.format("update:: JobExecutionSourceChunk with id '%s' was not found", jobExecutionChunk.getId());
            log.warn(errorMessage);
            promise.fail(new NotFoundException(errorMessage));
          } else {
            promise.complete(jobExecutionChunk);
          }
        });
    } catch (Exception e) {
      log.warn("update:: Error updating jobExecutionSourceChunk", e);
      promise.fail(e);
    }
    return promise.future();
  }

  @Override
  public Future<Boolean> delete(String id, String tenantId) {
    return pgClientFactory.createInstance(tenantId).delete(TABLE_NAME, id)
      .map(updateResult -> updateResult.rowCount() == 1);
  }

  @Override
  public Future<Boolean> isAllChunksProcessed(String jobExecutionId, String tenantId) {
    try {
      String query = String.format(IS_PROCESSING_COMPLETED_QUERY, jobExecutionId);
      return pgClientFactory.createInstance(tenantId).select(query)
        .map(resultSet -> resultSet.iterator().next().getBoolean(0));
    } catch (Exception e) {
      log.warn("isAllChunksProcessed:: Error while checking if processing is completed for JobExecution {}", jobExecutionId, e);
      return Future.failedFuture(e);
    }
  }

  @Override
  public Future<Boolean> deleteByJobExecutionId(String jobExecutionId, String tenantId) {
    try {
      Criteria idCrit = constructCriteria(JOB_EXECUTION_ID_FIELD, jobExecutionId).setJSONB(false);
      return pgClientFactory.createInstance(tenantId).delete(TABLE_NAME, new Criterion(idCrit))
        .map(updateResult -> updateResult.rowCount() != 0);
    } catch (Exception e) {
      log.warn("deleteByJobExecutionId:: Error deleting JobExecutionSourceChunks by JobExecution id {}", jobExecutionId, e);
      return Future.failedFuture(e);
    }
  }
}
