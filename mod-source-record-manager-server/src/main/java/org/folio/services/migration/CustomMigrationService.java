package org.folio.services.migration;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.folio.dbschema.Versioned;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.springframework.stereotype.Service;

@Log4j2
@Service
@RequiredArgsConstructor
public class CustomMigrationService {

  private final List<CustomMigration> migrationList;

  public Future<Void> doCustomMigrations(TenantAttributes attributes, String tenantId) {
    @SuppressWarnings("rawtypes")
    List<Future> futures = new ArrayList<>();
    for (var migration : migrationList) {
      if (isNew(attributes, migration.getFeatureVersion())) {
        log.info("Do custom migration [description: {}, tenant: {}]", migration.getDescription(), tenantId);
        futures.add(migration.migrate(tenantId));
      }
    }
    return CompositeFuture.all(futures).mapEmpty();
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
