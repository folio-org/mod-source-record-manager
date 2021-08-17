package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.Record;
import org.folio.dao.util.PostgresClientFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.Optional;

import static java.lang.String.format;
import static org.folio.rest.persist.PostgresClient.convertToPsqlStandard;

@Repository
public class MappingRuleDaoImpl implements MappingRuleDao {
  private static final Logger LOGGER = LogManager.getLogger();

  private static final String TABLE_NAME = "mapping_rules";
  private static final String RULES_JSON_FIELD = "mappingRules";
  private static final String RULES_TYPE_FIELD = "record_type";
  private static final String SELECT_BY_TYPE_QUERY = "SELECT jsonb FROM %s.%s WHERE record_type = %s limit 1";
  private static final String UPDATE_QUERY = "UPDATE %s.%s SET jsonb = jsonb_set(jsonb, '{mappingRules}', '%s')";
  private static final String INSERT_QUERY = "INSERT INTO %s.%s (jsonb, record_type) VALUES ($1, $2)";

  @Autowired
  private PostgresClientFactory pgClientFactory;

  @Override
  public Future<Optional<JsonObject>>get(String tenantId, Record.RecordType recordType) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String query = format(SELECT_BY_TYPE_QUERY, convertToPsqlStandard(tenantId), TABLE_NAME, recordType.toString());
      pgClientFactory.createInstance(tenantId).select(query, promise);
    } catch (Exception e) {
      LOGGER.error("Error getting mapping rules", e);
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
  public Future<String> save(JsonObject rules, String tenantId, Record.RecordType recordType) {
    Promise<String> promise = Promise.promise();
    try {
      String query = format(INSERT_QUERY, convertToPsqlStandard(tenantId), TABLE_NAME);
      Tuple queryParams = Tuple.of(new JsonObject().put(RULES_JSON_FIELD, rules), recordType.toString());
      pgClientFactory.createInstance(tenantId).execute(query, queryParams);
    } catch (Exception e) {
      LOGGER.error("Error saving rules", e);
      promise.fail(e);
    }
    promise.complete();
    return promise.future();
  }

  @Override
  public Future<JsonObject> update(JsonObject rules, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String query = format(UPDATE_QUERY, convertToPsqlStandard(tenantId), TABLE_NAME, rules);
    pgClientFactory.createInstance(tenantId).execute(query, promise);
    return promise.future().map(rules);
  }
}
