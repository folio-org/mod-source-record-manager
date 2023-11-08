package org.folio.services.migration;

import io.vertx.core.Future;

public interface CustomMigration {

  Future<Void> migrate(String tenantId);

  String getFeatureVersion();

  String getDescription();
}
