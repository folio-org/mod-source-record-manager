package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Promise;

import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Tuple;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import javax.ws.rs.NotFoundException;

import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk;
import org.folio.rest.jaxrs.model.JobExecutionSourceChunk.State;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.interfaces.Results;
import org.springframework.beans.factory.annotation.Autowired;
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
@Repository
public class JobExecutionSourceChunkDaoImpl implements JobExecutionSourceChunkDao {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final String TABLE_NAME = "job_execution_source_chunks";

  private static final String ID_FIELD = "id";
  private static final String JOB_EXECUTION_ID_FIELD = "jobexecutionid";
  private static final String CREATED_DATE_JSONB_FIELD = "createdDate";
  private static final String STATE_JSONB_FIELD = "state";
  private static final String CHUNK_SIZE_JSONB_FIELD = "chunkSize";
  private static final String LAST_JSONB_FIELD = "last";

  private static final String IS_PROCESSING_COMPLETED_QUERY = "SELECT is_processing_completed('%s');";
  private static final String ARE_THERE_ANY_ERRORS_DURING_PROCESSING_QUERY = "SELECT processing_contains_error_chunks('%s');";
  private static final String INSERT_QUERY = "INSERT INTO %s.%s (id, jsonb, jobExecutionId) VALUES ($1, $2, $3)";
  private static final String SELECT_QUERY = "SELECT * FROM %s.%s WHERE jobExecutionId = $1 AND jsonb->>'last' = $2 OFFSET $3 LIMIT $4";


  @Autowired
  private PostgresClientFactory pgClientFactory;

  @Override
  public Future<String> save(JobExecutionSourceChunk jobExecutionChunk, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      LOGGER.trace("save:: Saving jobExecutionSourceChunk {} for tenant {}", jobExecutionChunk.getId(), tenantId);
      String query = format(INSERT_QUERY, convertToPsqlStandard(tenantId), TABLE_NAME);
      Tuple queryParams = Tuple.of(
        StringUtils.defaultIfEmpty(jobExecutionChunk.getId(), /* generate UUID for the empty last chunk */ UUID.randomUUID().toString()),
        JsonObject.mapFrom(jobExecutionChunk),
        jobExecutionChunk.getJobExecutionId());
      pgClientFactory.createInstance(tenantId).execute(query, queryParams, promise);
    } catch (Exception e) {
      LOGGER.warn("save:: Failed to save JobExecutionSourceChunk with id: {}", jobExecutionChunk.getId(), e);
      promise.fail(e);
    }
    return promise.future().map(jobExecutionChunk.getId());
  }

  @Override
  public Future<List<JobExecutionSourceChunk>> get(String jobExecutionId, boolean isLast, int offset, int limit, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String query = format(SELECT_QUERY, convertToPsqlStandard(tenantId), TABLE_NAME);
      Tuple queryParams = Tuple.of(jobExecutionId, Boolean.valueOf(isLast).toString(), offset, limit);
      pgClientFactory.createInstance(tenantId).selectRead(query, queryParams, promise);
    } catch (Exception e) {
      LOGGER.warn("get:: Error while searching for JobExecutionSourceChunks", e);
      promise.fail(e);
    }
    return promise.future().map(this::mapResultSetToJobExecutionSourceChunks);
  }

  private List<JobExecutionSourceChunk> mapResultSetToJobExecutionSourceChunks(RowSet<Row> resultSet) {
    List<JobExecutionSourceChunk> result = new ArrayList<>();
    resultSet.forEach(row -> {
      JsonObject jsonb = row.getJsonObject("jsonb");
      JobExecutionSourceChunk chunk = new JobExecutionSourceChunk()
        .withId(row.getUUID(ID_FIELD).toString())
        .withJobExecutionId(row.getUUID(JOB_EXECUTION_ID_FIELD).toString())
        .withChunkSize(jsonb.getInteger(CHUNK_SIZE_JSONB_FIELD))
        .withState(State.fromValue(jsonb.getString(STATE_JSONB_FIELD)))
        .withCreatedDate(new Date(jsonb.getLong(CREATED_DATE_JSONB_FIELD)))
        .withLast(jsonb.getBoolean(LAST_JSONB_FIELD));
      result.add(chunk);
    });
    return result;
  }

  @Override
  public Future<Optional<JobExecutionSourceChunk>> getById(String id, String tenantId) {
    Promise<Results<JobExecutionSourceChunk>> promise = Promise.promise();
    try {
      if (StringUtils.isBlank(id)) {
        LOGGER.warn("getById:: Can't retrieve JobExecutionSourceChunk by empty id.");
        return promise.future().map(Optional.empty());
      }
      Criteria idCrit = constructCriteria(ID_FIELD, id).setJSONB(false);
      pgClientFactory.createInstance(tenantId)
        .get(TABLE_NAME, JobExecutionSourceChunk.class, new Criterion(idCrit), true, false, promise);
    } catch (Exception e) {
      LOGGER.warn("getById:: Error querying JobExecutionSourceChunk by id {}", id, e);
      promise.fail(e);
    }
    return promise.future()
      .map(Results::getResults)
      .map(jobExecutionSourceChunks -> jobExecutionSourceChunks.isEmpty() ? Optional.empty() : Optional.of(jobExecutionSourceChunks.get(0)));
  }

  @Override
  public Future<JobExecutionSourceChunk> update(JobExecutionSourceChunk jobExecutionChunk, String tenantId) {
    Promise<JobExecutionSourceChunk> promise = Promise.promise();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, jobExecutionChunk.getId()).setJSONB(false);
      pgClientFactory.createInstance(tenantId)
        .update(TABLE_NAME, jobExecutionChunk, new Criterion(idCrit), true, updateResult -> {
          if (updateResult.failed()) {
            LOGGER.warn("update:: Could not update jobExecutionSourceChunk with id {}", jobExecutionChunk.getId(), updateResult.cause());
            promise.fail(updateResult.cause());
          } else if (updateResult.result().rowCount() != 1) {
            String errorMessage = String.format("JobExecutionSourceChunk with id '%s' was not found", jobExecutionChunk.getId());
            LOGGER.warn(errorMessage);
            promise.fail(new NotFoundException(errorMessage));
          } else {
            promise.complete(jobExecutionChunk);
          }
        });
    } catch (Exception e) {
      LOGGER.warn("update:: Error updating jobExecutionSourceChunk", e);
      promise.fail(e);
    }
    return promise.future();
  }

  @Override
  public Future<Boolean> delete(String id, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    pgClientFactory.createInstance(tenantId).delete(TABLE_NAME, id, promise);
    return promise.future().map(updateResult -> updateResult.rowCount() == 1);
  }

  @Override
  public Future<Boolean> isAllChunksProcessed(String jobExecutionId, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String query = String.format(IS_PROCESSING_COMPLETED_QUERY, jobExecutionId);
      pgClientFactory.createInstance(tenantId).select(query, promise);
    } catch (Exception e) {
      LOGGER.warn("isAllChunksProcessed:: Error while checking if processing is completed for JobExecution {}", jobExecutionId, e);
      promise.fail(e);
    }
    return promise.future().map(resultSet -> resultSet.iterator().next().getBoolean(0));
  }

  @Override
  public Future<Boolean> containsErrorChunks(String jobExecutionId, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String query = String.format(ARE_THERE_ANY_ERRORS_DURING_PROCESSING_QUERY, jobExecutionId);
      pgClientFactory.createInstance(tenantId).select(query, promise);
    } catch (Exception e) {
      LOGGER.warn("containsErrorChunks:: Error while checking if any errors occurred for JobExecution {}", jobExecutionId, e);
      promise.fail(e);
    }
    return promise.future().map(resultSet -> resultSet.iterator().next().getBoolean(0));
  }

  @Override
  public Future<Boolean> deleteByJobExecutionId(String jobExecutionId, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      Criteria idCrit = constructCriteria(JOB_EXECUTION_ID_FIELD, jobExecutionId).setJSONB(false);
      pgClientFactory.createInstance(tenantId).delete(TABLE_NAME, new Criterion(idCrit), promise);
    } catch (Exception e) {
      LOGGER.warn("deleteByJobExecutionId:: Error deleting JobExecutionSourceChunks by JobExecution id {}", jobExecutionId, e);
      promise.fail(e);
    }
    return promise.future().map(updateResult -> updateResult.rowCount() != 0);
  }
}
