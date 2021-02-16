package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

import java.util.Map;
import javax.ws.rs.core.Response;

import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.services.MappingRuleService;
import org.folio.spring.SpringContextUtil;
import org.folio.util.pubsub.PubSubClientUtils;
import org.springframework.beans.factory.annotation.Autowired;

public class ModTenantAPI extends TenantAPI {
  private static final Logger LOGGER = LogManager.getLogger();

  private static final String GRANT_SEQUENCES_PERMISSION_PATTERN = "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA %s TO %s;";

  @Autowired
  private MappingRuleService mappingRuleService;

  public ModTenantAPI() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  Future<Integer> loadData(TenantAttributes attributes, String tenantId,
                           Map<String, String> headers, Context context) {
    return super.loadData(attributes, tenantId, headers, context)
      .compose(num -> setSequencesPermissionForDbUser(context, tenantId)
        .compose(ar -> mappingRuleService.saveDefaultRules(tenantId))
        .compose(ar -> registerModuleToPubsub(headers, context.owner())).map(num));
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

  private Future<Void> registerModuleToPubsub(Map<String, String> headers, Vertx vertx) {
    Promise<Void> promise = Promise.promise();
    PubSubClientUtils.registerModule(new org.folio.rest.util.OkapiConnectionParams(headers, vertx))
      .whenComplete((registrationAr, throwable) -> {
        if (throwable == null) {
          LOGGER.info("Module was successfully registered as publisher/subscriber in mod-pubsub");
          promise.complete();
        } else {
          LOGGER.error("Error during module registration in mod-pubsub", throwable);
          promise.fail(throwable);
        }
      });
    return promise.future();
  }

}
