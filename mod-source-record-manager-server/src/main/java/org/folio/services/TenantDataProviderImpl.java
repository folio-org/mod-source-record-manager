package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Row;
import org.folio.rest.persist.PostgresClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.StreamSupport;

@Service
public class TenantDataProviderImpl implements TenantDataProvider {
  private final Vertx vertx;

  @Autowired
  public TenantDataProviderImpl(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public Future<List<String>> getModuleTenants() {
    PostgresClient pgClient = PostgresClient.getInstance(vertx);
    String tenantQuery = "select nspname from pg_catalog.pg_namespace where nspname LIKE '%_mod_source_record_manager';";
    return pgClient.select(tenantQuery)
      .map(rowSet -> StreamSupport.stream(rowSet.spliterator(), false)
        .map(this::mapToTenant)
        .toList()
      );
  }

  private String mapToTenant(Row row) {
    String nsTenant = row.getString("nspname");
    String suffix = "_mod_source_record_manager";
    int tenantNameLength = nsTenant.length() - suffix.length();
    return nsTenant.substring(0, tenantNameLength);
  }
}
