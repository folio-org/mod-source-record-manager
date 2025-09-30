package org.folio.services.migration;

import io.vertx.core.Future;
import java.util.Comparator;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.folio.dao.RuleMigrationChangeLogDao;
import org.springframework.stereotype.Service;

@Log4j2
@Service
public class CustomMigrationService {

  private final List<CustomMigration> migrationList;

  private final RuleMigrationChangeLogDao ruleMigrationChangeLogDao;

  public CustomMigrationService(List<CustomMigration> migrationList, RuleMigrationChangeLogDao ruleMigrationChangeLogDao) {
    this.migrationList = migrationList.stream()
      .sorted(Comparator.comparing(CustomMigration::getOrder))
      .toList();
    this.ruleMigrationChangeLogDao = ruleMigrationChangeLogDao;
  }

  public Future<Void> doCustomMigrations(String tenantId) {
    return ruleMigrationChangeLogDao.getMigrationIds(tenantId)
      .compose(existingIds -> {
        log.info("Found {} migrated rule IDs", existingIds.size());
        var result = Future.<Void>succeededFuture();

        for (var migration : migrationList) {
          if (!existingIds.contains(migration.getMigrationId())) {
            log.info("Do custom migration [description: {}, tenant: {}]", migration.getDescription(), tenantId);
            result = runCustomMigration(tenantId, migration, result);
          } else {
            log.info("Skip already applied migration [description: {}, tenant: {}]",
              migration.getDescription(), tenantId);
          }
        }
        return result;
      })
      .onSuccess(v -> log.info("Custom migrations completed successfully for tenant {}", tenantId))
      .onFailure(e -> log.error("Custom migrations failed for tenant {}: {}", tenantId, e.getMessage()));
  }


  private Future<Void> runCustomMigration(String tenantId, CustomMigration migration, Future<Void> result) {
    return result
      .compose(v -> migration.migrate(tenantId))
      .compose(v -> {
        ruleMigrationChangeLogDao.save(migration, tenantId);
        return Future.succeededFuture();
      });
  }
}
