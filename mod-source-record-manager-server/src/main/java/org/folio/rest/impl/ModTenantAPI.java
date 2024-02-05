package org.folio.rest.impl;

import static org.folio.rest.tools.utils.TenantTool.tenantId;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import java.util.Map;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import javax.ws.rs.core.Response;
import org.folio.kafka.services.KafkaAdminClientService;
import org.folio.services.kafka.SRMKafkaTopicService;
import org.folio.services.migration.CustomMigrationService;
import org.springframework.beans.factory.annotation.Autowired;

import org.folio.Record;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.services.MappingRuleService;
import org.folio.spring.SpringContextUtil;

public class ModTenantAPI extends TenantAPI {

  private static final String GRANT_SEQUENCES_PERMISSION_PATTERN =
    "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA %s TO %s;";

  @Autowired
  private MappingRuleService mappingRuleService;
  @Autowired
  private CustomMigrationService customMigrationService;
  @Autowired
  private SRMKafkaTopicService srmKafkaTopicService;

  public ModTenantAPI() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  public void postTenant(TenantAttributes tenantAttributes, Map<String, String> headers,
                         Handler<AsyncResult<Response>> handler, Context context) {
    super.postTenant(tenantAttributes, headers, ar -> {
      if (ar.succeeded()) {
        Vertx vertx = context.owner();
        var tenantId = tenantId(headers);
        var kafkaAdminClientService = new KafkaAdminClientService(vertx);
        kafkaAdminClientService.createKafkaTopics(srmKafkaTopicService.createTopicObjects(), tenantId);
        handler.handle(Future.succeededFuture(ar.result()));
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    }, context);
  }

  @Override
  Future<Integer> loadData(TenantAttributes attributes, String tenantId,
                           Map<String, String> headers, Context context) {
    return super.loadData(attributes, tenantId, headers, context)
      .compose(num -> setSequencesPermissionForDbUser(context, tenantId)
        .compose(ar -> mappingRuleService.saveDefaultRules(Record.RecordType.MARC_BIB, tenantId))
        .compose(ar -> mappingRuleService.saveDefaultRules(Record.RecordType.MARC_HOLDING, tenantId))
        .compose(ar -> mappingRuleService.saveDefaultRules(Record.RecordType.MARC_AUTHORITY, tenantId))
        .compose(ar -> customMigrationService.doCustomMigrations(attributes, tenantId))
        .compose(ar -> saveTenantId(tenantId, context))
        .map(num));
  }

  private Future<Void> saveTenantId(String tenantId, Context context) {
    Vertx owner = context.owner();
    owner.sharedData().getLocalMap("tenants").put(tenantId, tenantId);
    return Future.succeededFuture();
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
