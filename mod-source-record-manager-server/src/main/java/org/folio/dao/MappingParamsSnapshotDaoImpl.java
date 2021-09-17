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
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;

import static java.lang.String.format;
import static org.folio.rest.persist.PostgresClient.convertToPsqlStandard;

@Repository
public class MappingParamsSnapshotDaoImpl implements MappingParamsSnapshotDao {

  @Autowired
  private PostgresClientFactory pgClientFactory;

  private static final Logger LOGGER = LogManager.getLogger();

  private static final String TABLE_NAME = "mapping_params_snapshots";
  private static final String SELECT_QUERY = "SELECT params FROM %s.%s WHERE job_execution_id = $1";
  private static final String INSERT_SQL = "INSERT INTO %s.%s (job_execution_id, params, saved_timestamp) VALUES ($1, $2, $3)";
  private static final String DELETE_BY_JOB_EXECUTION_ID_QUERY = "DELETE FROM %s.%s WHERE job_execution_id = $1";
  private static final String PARAMS_FIELD = "params";

  @Override
  public Future<Optional<MappingParameters>> getByJobExecutionId(String jobExecutionId, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String query = format(SELECT_QUERY, convertToPsqlStandard(tenantId), TABLE_NAME);
    Tuple queryParams = Tuple.of(UUID.fromString(jobExecutionId));
    pgClientFactory.createInstance(tenantId).select(query, queryParams, promise);
    return promise.future().map(resultSet -> {
      if (resultSet.rowCount() == 0 || resultSet.iterator().next().getValue(PARAMS_FIELD) == null) {
        return Optional.empty();
      } else {
        MappingParameters rules = resultSet.iterator().next().getJsonObject(PARAMS_FIELD).mapTo(MappingParameters.class);
        return Optional.of(rules);
      }
    });
  }

  @Override
  public Future<String> save(MappingParameters params, String jobExecutionId, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String query = format(INSERT_SQL, convertToPsqlStandard(tenantId), TABLE_NAME);
      Tuple queryParams = Tuple.of(
        UUID.fromString(jobExecutionId),
        JsonObject.mapFrom(params),
        LocalDateTime.now()
      );
      pgClientFactory.createInstance(tenantId).execute(query, queryParams, promise);
    } catch (Exception e) {
      LOGGER.error("Error saving MappingParamsSnapshot entity", e);
      promise.fail(e);
    }
    return promise.future().map(jobExecutionId).onFailure(e -> LOGGER.error("Failed to save MappingParamsSnapshot entity", e));
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
