package org.folio.services.migration;

import io.vertx.core.Future;
import org.folio.Record;

import java.util.UUID;

public interface CustomMigration {

  Future<Void> migrate(String tenantId);

  UUID getMigrationId();

  int getOrder();

  Record.RecordType getRecordType();

  String getDescription();
}
