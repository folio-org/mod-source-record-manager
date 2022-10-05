package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.PostgresClientFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import static java.lang.String.format;
import static org.folio.rest.persist.PostgresClient.convertToPsqlStandard;

@Repository
public class MappingRulesSnapshotDaoImpl implements MappingRulesSnapshotDao {

  @Autowired
  private PostgresClientFactory pgClientFactory;

  private static final Logger LOGGER = LogManager.getLogger();

  private static final String TABLE_NAME = "mapping_rules_snapshots";
  private static final String SELECT_QUERY = "SELECT rules FROM %s.%s WHERE job_execution_id = $1";
  private static final String INSERT_SQL = "INSERT INTO %s.%s (job_execution_id, rules, saved_timestamp) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING";
  private static final String DELETE_BY_JOB_EXECUTION_ID_QUERY = "DELETE FROM %s.%s WHERE job_execution_id = $1";
  private static final String RULES_FIELD = "rules";

  @Override
  public Future<Optional<JsonObject>> getByJobExecutionId(String jobExecutionId, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String query = format(SELECT_QUERY, convertToPsqlStandard(tenantId), TABLE_NAME);
    Tuple queryParams = Tuple.of(UUID.fromString(jobExecutionId));
    pgClientFactory.createInstance(tenantId).selectRead(query, queryParams, promise);
    return promise.future().map(resultSet -> {
      if (resultSet.rowCount() == 0 || resultSet.iterator().next().getValue(RULES_FIELD) == null) {
        return Optional.empty();
      } else {
        JsonObject rules = new JsonObject(resultSet.iterator().next().getValue(RULES_FIELD).toString());
        return Optional.of(rules);
      }
    });
  }

  @Override
  public Future<String> save(JsonObject rules, String jobExecutionId, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String query = format(INSERT_SQL, convertToPsqlStandard(tenantId), TABLE_NAME);
      Tuple queryParams = Tuple.of(
        UUID.fromString(jobExecutionId),
        rules,
        LocalDateTime.now()
      );
      pgClientFactory.createInstance(tenantId).execute(query, queryParams, promise);
    } catch (Exception e) {
      LOGGER.error("Error saving MappingRulesSnapshot entity", e);
      promise.fail(e);
    }
    return promise.future().map(jobExecutionId).onFailure(e -> LOGGER.error("Failed to save MappingRulesSnapshot entity", e));
  }

  @Override
  public Future<Boolean> delete(String jobExecutionId, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String query = format(DELETE_BY_JOB_EXECUTION_ID_QUERY, convertToPsqlStandard(tenantId), TABLE_NAME);
    Tuple queryParams = Tuple.of(UUID.fromString(jobExecutionId));
    pgClientFactory.createInstance(tenantId).execute(query, queryParams, promise);
    return promise.future().map(updateResult -> updateResult.rowCount() == 1);
  }

}


