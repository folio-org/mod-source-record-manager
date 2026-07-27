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
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.stereotype.Repository;

import org.folio.Record;
import org.folio.dao.util.PostgresClientFactory;

@Log4j2
@Repository
@RequiredArgsConstructor
public class MappingRuleDaoImpl implements MappingRuleDao {

  private static final String TABLE_NAME = "mapping_rules";
  private static final String RULES_JSON_FIELD = "mappingRules";
  private static final String SELECT_BY_TYPE_QUERY = "SELECT jsonb FROM %s.%s WHERE record_type = $1 limit 1";
  private static final String UPDATE_QUERY = "UPDATE %s.%s SET jsonb = $1 WHERE record_type = $2";
  private static final String INSERT_QUERY = "INSERT INTO %s.%s (id, jsonb, record_type) VALUES ($1, $2, $3)";

  private final PostgresClientFactory pgClientFactory;

  @Override
  public Future<Optional<JsonObject>> get(Record.RecordType recordType, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String query = format(SELECT_BY_TYPE_QUERY, convertToPsqlStandard(tenantId), TABLE_NAME);
      Tuple queryParams = Tuple.of(recordType != null ? recordType.toString() : null);
      pgClientFactory.createInstance(tenantId).selectRead(query, queryParams, promise::handle);
    } catch (Exception e) {
      log.warn("get:: Error getting mapping rules", e);
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
    log.trace("save:: Saving mapping rules tenant id {}", tenantId);
    UUID id = UUID.randomUUID();
    String query = format(INSERT_QUERY, convertToPsqlStandard(tenantId), TABLE_NAME);
    Tuple queryParams = Tuple.of(
      id,
      new JsonObject().put(RULES_JSON_FIELD, rules),
      recordType.toString());
    return pgClientFactory.createInstance(tenantId).execute(query, queryParams)
      .onFailure(e -> log.warn("save:: Error saving rules", e))
      .map(id.toString());
  }

  @Override
  public Future<JsonObject> update(JsonObject rules, Record.RecordType recordType, String tenantId) {
    String query = format(UPDATE_QUERY, convertToPsqlStandard(tenantId), TABLE_NAME);
    Tuple queryParams = Tuple.of(new JsonObject().put(RULES_JSON_FIELD, rules), recordType.toString());
    return pgClientFactory.createInstance(tenantId).execute(query, queryParams)
      .onFailure(e -> log.warn("update:: Error updating rules", e))
      .map(rules);
  }
}
