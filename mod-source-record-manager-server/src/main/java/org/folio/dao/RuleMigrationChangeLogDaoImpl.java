package org.folio.dao;

import static java.lang.String.format;
import static org.folio.rest.persist.PostgresClient.convertToPsqlStandard;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.StreamSupport;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.services.migration.CustomMigration;
import org.springframework.stereotype.Repository;

@Repository
public class RuleMigrationChangeLogDaoImpl implements RuleMigrationChangeLogDao {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final String TABLE_NAME = "rule_migration_change_log";
  private static final String MIGRATION_ID_FIELD = "migration_id";
  private static final String SELECT_QUERY = "SELECT migration_id FROM %s.%s";
  private static final String INSERT_QUERY =
    """
       INSERT INTO %s.%s (id, migration_id, record_type, file_name, description, timestamp)
       VALUES ($1, $2, $3, $4, $5, $6)
      """;

  private final PostgresClientFactory pgClientFactory;

  public RuleMigrationChangeLogDaoImpl(PostgresClientFactory pgClientFactory) {
    this.pgClientFactory = pgClientFactory;
  }

  @Override
  public Future<List<UUID>> getMigrationIds(String tenantId) {
    LOGGER.trace("getMigrationIds:: Getting migrationIds for tenant {}", tenantId);

    Promise<RowSet<Row>> promise = Promise.promise();
    var query = format(SELECT_QUERY, convertToPsqlStandard(tenantId), TABLE_NAME);

    pgClientFactory.createInstance(tenantId).selectRead(query, Tuple.tuple(), promise);

    return promise.future().map(rows -> {
      if (rows == null) {
        return Collections.emptyList();
      }
      return StreamSupport.stream(rows.spliterator(), false)
        .map(row -> row.getUUID(MIGRATION_ID_FIELD))
        .filter(Objects::nonNull)
        .toList();
    });
  }

  @Override
  public Future<Void> save(CustomMigration migration, String tenantId) {
    var query = format(INSERT_QUERY, convertToPsqlStandard(tenantId), TABLE_NAME);
    var queryParams = Tuple.of(
      UUID.randomUUID(),
      migration.getMigrationId(),
      migration.getRecordType(),
      migration.getClass().getSimpleName(),
      migration.getDescription(),
      LocalDateTime.now());
    try {
      LOGGER.trace("save:: Saving RuleMigrationChangeLog with migrationId: {} for tenant {}",
        migration.getMigrationId(), tenantId);

      return pgClientFactory.createInstance(tenantId)
        .execute(query, queryParams)
        .mapEmpty();
    } catch (Exception e) {
      LOGGER.error("save:: Failed to save RuleMigrationChangeLog with migrationId: {} for tenant {}. Error: {}",
        migration.getMigrationId(), tenantId, e);
      return Future.failedFuture(e);
    }
  }
}
