package org.folio.rest.impl;

import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import java.util.Map;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.services.MappingRuleService;
import org.folio.spring.SpringContextUtil;
import org.folio.verticle.JobMonitoringWatchdogVerticle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

public class ModTenantAPI extends TenantAPI {

  private static final String GRANT_SEQUENCES_PERMISSION_PATTERN =
    "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA %s TO %s;";

  @Autowired
  private MappingRuleService mappingRuleService;

  @Value("${srm.kafka.QuickMarcUpdateConsumersVerticle.instancesNumber:1}")
  private int jobExecutionWatchdogInstanceNumber;

  public ModTenantAPI() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  Future<Integer> loadData(TenantAttributes attributes, String tenantId,
                           Map<String, String> headers, Context context) {
    return super.loadData(attributes, tenantId, headers, context)
      .compose(num -> setSequencesPermissionForDbUser(context, tenantId)
        .compose(ar -> mappingRuleService.saveDefaultRules(tenantId))
        .compose(ar -> deployVerticle(tenantId, context))
        .map(num));
  }

  private Future<Void> deployVerticle(String tenantId, Context context) {
    Vertx owner = saveTenant(tenantId, context);

    JobMonitoringWatchdogVerticle.setSpringContext(owner.getOrCreateContext().get("springContext"));
    Promise<String> deployJobExecutionWatchdog = Promise.promise();

    owner.deployVerticle("org.folio.verticle.JobMonitoringWatchdogVerticle",
      new DeploymentOptions()
        .setWorker(true)
        .setInstances(jobExecutionWatchdogInstanceNumber), deployJobExecutionWatchdog);

    return Future.succeededFuture(null);
  }

  private Vertx saveTenant(String tenantId, Context context) {
    Vertx owner = context.owner();
    LocalMap<String, String> tenants = owner.sharedData().getLocalMap("tenants");
    tenants.put("tenant", tenantId);
    return owner;
  }

  private Future<RowSet<Row>> setSequencesPermissionForDbUser(Context context, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String dbSchemaName = new StringBuilder()
      .append(tenantId)
      .append('_')
      .append(PostgresClient.getModuleName()).toString();
    String preparedSql = String.format(GRANT_SEQUENCES_PERMISSION_PATTERN, dbSchemaName, dbSchemaName);
    PostgresClient.getInstance(context.owner()).execute(preparedSql, promise);
    return promise.future();
  }



}
