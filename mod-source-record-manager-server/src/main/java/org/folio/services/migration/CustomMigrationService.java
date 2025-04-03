package org.folio.services.migration;

import io.vertx.core.Future;
import java.util.Comparator;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.folio.dbschema.Versioned;
import org.folio.okapi.common.SemVer;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.springframework.stereotype.Service;

@Log4j2
@Service
public class CustomMigrationService {

  private final List<CustomMigration> migrationList;

  public CustomMigrationService(List<CustomMigration> migrationList) {
    this.migrationList = migrationList.stream()
      .sorted(Comparator.comparing(o -> new SemVer(o.getFeatureVersion())))
      .toList();
  }

  public Future<Void> doCustomMigrations(TenantAttributes attributes, String tenantId) {
    var result = Future.<Void>succeededFuture();
    for (var migration : migrationList) {
      if (isNew(attributes, migration.getFeatureVersion())) {
        log.info("Do custom migration [description: {}, tenant: {}]", migration.getDescription(), tenantId);
        result = result.compose(o -> migration.migrate(tenantId));
      }
    }

    return result
      .onComplete(ar -> {
        if (ar.succeeded()) {
          log.info("Custom migrations completed successfully for tenant {}", tenantId);
        } else {
          log.error("Custom migrations failed for tenant {}: {}", tenantId, ar.cause().getMessage());
        }
      });
  }

  private static boolean isNew(TenantAttributes attributes, String featureVersion) {
    if (attributes.getModuleFrom() == null) {
      return true;
    }
    var since = new Versioned() { };
    since.setFromModuleVersion(featureVersion);
    return since.isNewForThisInstall(attributes.getModuleFrom());
  }
}
