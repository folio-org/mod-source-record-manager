package org.folio.dao;

import io.vertx.core.Future;
import java.util.List;
import java.util.UUID;
import org.folio.services.migration.CustomMigration;

public interface RuleMigrationChangeLogDao {
  /**
   * Returns all migration ids from the rule_migration_change_log table
   *
   * @return future with list of migration ids
   */
  Future<List<UUID>> getMigrationIds(String tenantId);

  /**
   * Saves a new record to the rule_migration_change_log table
   *
   * @param migration   the migration details
   * @param tenantId the tenant id
   * @return future of Void
   */
  Future<Void> save(CustomMigration migration, String tenantId);

}
