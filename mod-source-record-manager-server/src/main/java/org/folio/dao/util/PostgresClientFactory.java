
package org.folio.dao.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgPool;
import io.vertx.pgclient.impl.PgPoolImpl;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.impl.PoolBase;
import io.vertx.sqlclient.impl.PoolImpl;
import io.vertx.sqlclient.impl.pool.SqlConnectionPool;
import org.apache.commons.collections4.map.MultiKeyMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.rest.persist.PostgresClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.stream.Collectors;

@Component
public class PostgresClientFactory {

  private static final Logger LOGGER = LogManager.getLogger();

  private Vertx vertx;

  public PostgresClientFactory(@Autowired Vertx vertx) {
    this.vertx = vertx;
  }

  /**
   * Creates instance of Postgres Client
   *
   * @param tenantId tenant id
   * @return Postgres Client
   */
  public PostgresClient createInstance(String tenantId) {
    LOGGER.warn("createInstance:: getPostgresClient");
    PostgresClient postgresClient = PostgresClient.getInstance(vertx, tenantId);
    LOGGER.warn("connection: postgresClient {}", postgresClient);

    try {
      Field connectionPoolField = PostgresClient.class.getDeclaredField("CONNECTION_POOL");
      connectionPoolField.setAccessible(true);
      MultiKeyMap<Object, PostgresClient> CONNECTION_POOL = (MultiKeyMap<Object, PostgresClient>)connectionPoolField.get(postgresClient);
      LOGGER.warn("connection: CONNECTION_POOL.Keys {}", CONNECTION_POOL.keySet().size());
      LOGGER.warn("connection: CONNECTION_POOL.Values {}", CONNECTION_POOL.values().size());

      CONNECTION_POOL.entrySet().stream().forEach(e -> LOGGER.warn("connection: CONNECTION_POOL.key {}", e));
      CONNECTION_POOL.values().stream().forEach(e -> LOGGER.warn("connection: CONNECTION_POOL.value {}", e));

      PostgresClient pgClient = CONNECTION_POOL.values().stream().findFirst().get();
      LOGGER.warn("connection: Values {}", CONNECTION_POOL.values().size());

      Method getConnectionPoolSizeMethod = PostgresClient.class.getDeclaredMethod("getConnectionPoolSize");
      getConnectionPoolSizeMethod.setAccessible(true);
      int size = (int)getConnectionPoolSizeMethod.invoke(null, null);
      LOGGER.warn("connection: getConnectionPoolSize = {}", size);

      Method getClientMethod = PostgresClient.class.getDeclaredMethod("getClient");
      getClientMethod.setAccessible(true);
      io.vertx.pgclient.impl.PgPoolImpl pgPool = (PgPoolImpl)getClientMethod.invoke(postgresClient, null);
      LOGGER.warn("connection: pgPool = {}", pgPool);
      LOGGER.warn("connection: pgPool.size = {}", pgPool.size());

      Field poolField = PgPoolImpl.class.getSuperclass().getDeclaredFields()[2];
      poolField.setAccessible(true);
      io.vertx.sqlclient.impl.PoolImpl poolImpl = (PoolImpl) poolField.get(pgPool);
      LOGGER.warn("connection: poolImpl.size() = {}", poolImpl.size());

      Field sqlConnectionPoolField = PoolImpl.class.getDeclaredField("pool");
      sqlConnectionPoolField.setAccessible(true);
      SqlConnectionPool sqlConnectionPool = (SqlConnectionPool)sqlConnectionPoolField.get(poolImpl);
      LOGGER.warn("connection: SqlConnectionPool pool = {}", sqlConnectionPool);
      LOGGER.warn("connection: SqlConnectionPool pool.available() = {}", sqlConnectionPool.available());

//      Field pgPoolsField = PostgresClient.class.getDeclaredField("PG_POOLS");
//      pgPoolsField.setAccessible(true);
//
//      Field modifiers = Field.class.getDeclaredField("modifiers");
//      modifiers.setAccessible(true);
//      Map<Vertx, PgPool> PG_POOLS = (Map<Vertx,PgPool>)pgPoolsField.get(postgresClient);
//      LOGGER.warn("connection: PG_POOLS.Keys {}", PG_POOLS.keySet().size());
//      LOGGER.warn("connection: PG_POOLS.Values {}", PG_POOLS.values().size());
//      PG_POOLS.entrySet().stream().forEach(e -> LOGGER.warn("connection: PG_POOLS.key {}", e));
//      PG_POOLS.values().stream().forEach(e -> LOGGER.warn("connection: PG_POOLS.value {}", e));

    } catch (NoSuchFieldException | NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
      LOGGER.error("Access to private field", e);
    }


    //getAllConnectionNumber(postgresClient).onSuccess(res -> LOGGER.warn("all connection number = {}", res));
    //getUsedConnectionNumber(postgresClient).onSuccess(res -> LOGGER.warn("active connection number = {}", res));
    return postgresClient;
  }

  List<Field> getAllFields(Class clazz) {
    if (clazz == null) {
      return Collections.emptyList();
    }

    List<Field> result = new ArrayList<>(getAllFields(clazz.getSuperclass()));
    List<Field> filteredFields = Arrays.stream(clazz.getDeclaredFields())
      .filter(f -> Modifier.isPublic(f.getModifiers()) || Modifier.isProtected(f.getModifiers()))
      .collect(Collectors.toList());
    result.addAll(filteredFields);
    return result;
  }

  private Future<Integer> getUsedConnectionNumber(PostgresClient postgresClient) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      postgresClient.execute("select count(state) from pg_stat_activity where state = 'active';", promise);
    } catch (Exception e) {
      LOGGER.warn("updateCounterValue:: Failed to get active connection number", e);
      promise.fail(e);
    }
    return getCounterValueFromRowSet(promise);
  }

  private Future<Integer> getAllConnectionNumber(PostgresClient postgresClient) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      postgresClient.execute("select count(state) from pg_stat_activity;", promise);
    } catch (Exception e) {
      LOGGER.warn("updateCounterValue:: Failed to get all connection number", e);
      promise.fail(e);
    }
    return getCounterValueFromRowSet(promise);
  }

  private Future<Integer> getCounterValueFromRowSet(Promise<RowSet<Row>> promise) {
    return promise.future().map(resultSet -> resultSet.iterator().next().getInteger(0));
  }

}
