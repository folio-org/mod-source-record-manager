package org.folio.dao;

import static java.lang.String.format;

import static org.folio.rest.persist.PostgresClient.convertToPsqlStandard;

import java.util.Optional;
import java.util.UUID;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import org.folio.Record;
import org.folio.dao.util.PostgresClientFactory;

@Repository
public class MappingRuleDaoImpl implements MappingRuleDao {
  private static final Logger LOGGER = LogManager.getLogger();

  private static final String TABLE_NAME = "mapping_rules";
  private static final String RULES_JSON_FIELD = "mappingRules";
  private static final String SELECT_BY_TYPE_QUERY = "SELECT jsonb FROM %s.%s WHERE record_type = $1 limit 1";
  private static final String UPDATE_QUERY = "UPDATE %s.%s SET jsonb = $1 WHERE record_type = $2";
  private static final String INSERT_QUERY = "INSERT INTO %s.%s (id, jsonb, record_type) VALUES ($1, $2, $3)";

  @Autowired
  private PostgresClientFactory pgClientFactory;

  @Override
  public Future<Optional<JsonObject>> get(Record.RecordType recordType, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String query = format(SELECT_BY_TYPE_QUERY, convertToPsqlStandard(tenantId), TABLE_NAME);
      Tuple queryParams = Tuple.of(recordType != null ? recordType.toString() : null);
      pgClientFactory.createInstance(tenantId).selectRead(query, queryParams, promise);
    } catch (Exception e) {
      LOGGER.warn("get:: Error getting mapping rules", e);
      promise.fail(e);
    }
    return promise.future().map(resultSet -> {
      if (resultSet.rowCount() == 0) {
        return Optional.empty();
      } else {
        JsonObject rules = new JsonObject(resultSet.iterator().next().getValue("jsonb").toString())
          .getJsonObject(RULES_JSON_FIELD);
        return Optional.of(rules);
      }
    });
  }

  @Override
  public Future<String> save(JsonObject rules, Record.RecordType recordType, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    LOGGER.trace("save:: Saving mapping rules tenant id {}", tenantId);
    UUID id = UUID.randomUUID();
    String query = format(INSERT_QUERY, convertToPsqlStandard(tenantId), TABLE_NAME);
    Tuple queryParams = Tuple.of(
      id,
      new JsonObject().put(RULES_JSON_FIELD, rules),
      recordType.toString());
    pgClientFactory.createInstance(tenantId).execute(query, queryParams, promise);
    return promise.future().map(id.toString()).onFailure(e -> LOGGER.warn("save:: Error saving rules", e));
  }

  @Override
  public Future<JsonObject> update(JsonObject rules, Record.RecordType recordType, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String query = format(UPDATE_QUERY, convertToPsqlStandard(tenantId), TABLE_NAME);
    Tuple queryParams = Tuple.of(new JsonObject().put(RULES_JSON_FIELD, rules), recordType.toString());
    pgClientFactory.createInstance(tenantId).execute(query, queryParams, promise);
    return promise.future().map(rules).onFailure(e -> LOGGER.warn("update:: Error updating rules", e));
  }
}
