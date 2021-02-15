package org.folio.dao;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.DbUtil;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.SourceRecordState;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.SQLConnection;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;
import org.folio.rest.tools.utils.ValidationHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.ws.rs.NotFoundException;
import java.util.Optional;
import java.util.UUID;

import static org.folio.dataimport.util.DaoUtil.constructCriteria;

@Repository
public class SourceRecordStateDaoImpl implements SourceRecordStateDao {
  private static final Logger LOGGER = LogManager.getLogger();

  private static final String TABLE_NAME = "source_records_state";
  private static final String ID_FIELD = "'sourceRecordId'";

  @Autowired
  private PostgresClientFactory pgClientFactory;


  @Override
  public Future<Optional<SourceRecordState>> get(String sourceRecordId, String tenantId) {
    Promise<Results<SourceRecordState>> promise = Promise.promise();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, sourceRecordId);
      pgClientFactory.createInstance(tenantId).get(TABLE_NAME, SourceRecordState.class, new Criterion(idCrit), true, false, promise);
    } catch (Exception e) {
      LOGGER.error("Error getting sourceRecord state by source id", e);
      promise.fail(e);
    }
    return promise.future()
      .map(Results::getResults)
      .map(sourceRecordStates -> sourceRecordStates.isEmpty() ? Optional.empty() : Optional.of(sourceRecordStates.get(0)));
  }

  private Future<Optional<SourceRecordState>> get(String sourceRecordId, AsyncResult<SQLConnection> conn, String tenantId) {
    Promise<Results<SourceRecordState>> promise = Promise.promise();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, sourceRecordId);
      pgClientFactory.createInstance(tenantId).get(conn, TABLE_NAME, SourceRecordState.class, new Criterion(idCrit), true, false, promise);
    } catch (Exception e) {
      LOGGER.error("Error getting sourceRecord state by source id", e);
      promise.fail(e);
    }
    return promise.future()
      .map(Results::getResults)
      .map(sourceRecordStates -> sourceRecordStates.isEmpty() ? Optional.empty() : Optional.of(sourceRecordStates.get(0)));
  }

  @Override
  public Future<String> save(SourceRecordState state, String tenantId) {
    state.withId(UUID.randomUUID().toString());
    PostgresClient client = pgClientFactory.createInstance(tenantId);
    return DbUtil.executeInTransaction(client, sqlConnection -> get(state.getSourceRecordId(), sqlConnection, tenantId)
      .compose(sourceRecordStateOptional -> {
        if (sourceRecordStateOptional.isPresent()) {
          return update(state, sqlConnection, tenantId)
            .map(state.getId());
        } else {
          return save(state, sqlConnection, tenantId);
        }
      }));
  }

  public Future<String> save(SourceRecordState state, AsyncResult<SQLConnection> conn, String tenantId) {
    Promise<String> promise = Promise.promise();
    state.withId(UUID.randomUUID().toString());
    pgClientFactory.createInstance(tenantId).save(conn, TABLE_NAME, state.getId(), state, ar -> {
      if (ar.succeeded()) {
        promise.complete(state.getId());
      } else {
        if (ValidationHelper.isDuplicate(ar.cause().getMessage())) {
          promise.complete(state.getId());
        } else {
          LOGGER.error("Error saving SourceRecordState entity", ar.cause());
          promise.fail(ar.cause());
        }
      }
    });
    return promise.future();
  }

  @Override
  public Future<SourceRecordState> updateState(String sourceId, SourceRecordState.RecordState recordState, String tenantId) {
    PostgresClient client = pgClientFactory.createInstance(tenantId);
    return DbUtil.executeInTransaction(client, sqlConnection -> get(sourceId, sqlConnection, tenantId)
      .map(sourceRecordState -> sourceRecordState.orElseThrow(NotFoundException::new))
      .compose(sourceRecordState -> update(sourceRecordState.withRecordState(recordState), sqlConnection, tenantId)));
  }

  private Future<SourceRecordState> update(SourceRecordState state, AsyncResult<SQLConnection> conn, String tenantId) {
    Promise<SourceRecordState> promise = Promise.promise();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, state.getSourceRecordId());
      CQLWrapper wrapper = new CQLWrapper(new Criterion(idCrit));
      pgClientFactory.createInstance(tenantId).update(conn, TABLE_NAME, state, wrapper, true, updateResult -> {
        if (updateResult.failed()) {
          LOGGER.error("Could not update state SourceRecordState sourceRecordId {}", state.getSourceRecordId(), updateResult.cause());
          promise.fail(updateResult.cause());
        } else if (updateResult.result().rowCount() != 1) {
          String errorMessage = String.format("SourceRecordState with sourceRecordId '%s' was not found", state.getSourceRecordId());
          LOGGER.error(errorMessage);
          promise.fail(new NotFoundException(errorMessage));
        } else {
          promise.complete(state);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Error updating SourceRecordState", e);
      promise.fail(e);
    }
    return promise.future();
  }
}
