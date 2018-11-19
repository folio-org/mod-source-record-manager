package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.sql.UpdateResult;
import org.folio.dao.util.DaoUtil;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;

import java.util.List;
import java.util.Optional;

import static org.folio.dao.util.DaoUtil.constructCriteria;

/**
 * Abstract Layer for Generic DAO, works with Database using PostgresClient.
 *
 * @param <ENTITY> entire entity type
 * @see PostgresClient
 */
public abstract class AbstractGenericDao<ENTITY> implements GenericDao<ENTITY> {

  private final String DEFAULT_ID_FIELD = "'id'";

  protected PostgresClient pgClient;
  protected Class<ENTITY> entityClass;

  public AbstractGenericDao(Vertx vertx, String tenantId, Class<ENTITY> entityClass) {
    this.pgClient = PostgresClient.getInstance(vertx, tenantId);
    this.entityClass = entityClass;
  }

  @Override
  public Future<List<ENTITY>> getByQuery(String query, int offset, int limit) {
    Future<Results<ENTITY>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cql = DaoUtil.getCQLWrapper(getTableName(), query, limit, offset);
      pgClient.get(getTableName(), entityClass, fieldList, cql, true, false, future.completer());
    } catch (Exception e) {
      future.fail(e);
    }
    return future.map(Results::getResults);
  }

  @Override
  public Future<Optional<ENTITY>> getById(String id) {
    Future<Results<ENTITY>> future = Future.future();
    try {
      Criteria criteria = constructCriteria(getSchemaPath(), getIdField(), id);
      pgClient.get(getTableName(), entityClass, new Criterion(criteria), true, false, future.completer());
    } catch (Exception e) {
      future.fail(e);
    }
    return future
      .map(Results::getResults)
      .map(entities -> entities.isEmpty() ? Optional.empty() : Optional.of(entities.get(0)));
  }

  @Override
  public Future<ENTITY> save(String id, ENTITY entity) {
    Future<String> future = Future.future();
    pgClient.save(getTableName(), id, entity, future.completer());
    return future.map(entity);
  }

  @Override
  public Future<ENTITY> update(String id, ENTITY entity) {
    Future<UpdateResult> future = Future.future();
    try {
      Criteria criteria = constructCriteria(getSchemaPath(), getIdField(), id);
      pgClient.update(getTableName(), entity, new Criterion(criteria), true, future.completer());
    } catch (Exception e) {
      future.fail(e);
    }
    return future.map(entity);
  }

  @Override
  public Future<Boolean> delete(String id) {
    Future<UpdateResult> future = Future.future();
    pgClient.delete(getTableName(), id, future.completer());
    return future.map(updateResult -> updateResult.getUpdated() == 1);
  }

  protected String getIdField() {
    return DEFAULT_ID_FIELD;
  }

  protected abstract String getTableName();

  protected abstract String getSchemaPath();
}
