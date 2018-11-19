package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.UpdateResult;
import org.folio.dao.util.DaoUtil;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;

import java.util.Optional;

import static org.folio.dao.util.DaoUtil.constructCriteria;

/**
 * Abstract Layer for Generic DAO, works with Database using PostgresClient.
 *
 * @param <E> dedicated entity
 * @see PostgresClient
 */
public abstract class AbstractGenericDao<E> implements GenericDao<E> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractGenericDao.class);

  private static final String DEFAULT_ID_FIELD = "'id'";

  protected PostgresClient pgClient;
  protected Class<E> entityClass;

  public AbstractGenericDao(Vertx vertx, String tenantId, Class<E> entityClass) {
    this.pgClient = PostgresClient.getInstance(vertx, tenantId);
    this.entityClass = entityClass;
  }

  @Override
  public Future<Results<E>> getByQuery(String query, int offset, int limit) {
    Future<Results<E>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cql = DaoUtil.getCQLWrapper(getTableName(), query, limit, offset);
      pgClient.get(getTableName(), entityClass, fieldList, cql, true, false, future.completer());
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      future.fail(e);
    }
    return future;
  }

  @Override
  public Future<Optional<E>> getById(String id) {
    Future<Results<E>> future = Future.future();
    try {
      Criteria criteria = constructCriteria(getSchemaPath(), getIdField(), id);
      pgClient.get(getTableName(), entityClass, new Criterion(criteria), true, false, future.completer());
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
      future.fail(e);
    }
    return future
      .map(Results::getResults)
      .map(entities -> entities.isEmpty() ? Optional.empty() : Optional.of(entities.get(0)));
  }

  @Override
  public Future<E> save(String id, E entity) {
    Future<String> future = Future.future();
    pgClient.save(getTableName(), id, entity, future.completer());
    return future.map(entity);
  }

  @Override
  public Future<E> update(String id, E entity) {
    Future<UpdateResult> future = Future.future();
    try {
      Criteria criteria = constructCriteria(getSchemaPath(), getIdField(), id);
      pgClient.update(getTableName(), entity, new Criterion(criteria), true, future.completer());
    } catch (Exception e) {
      LOGGER.error(e.getMessage(), e);
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
