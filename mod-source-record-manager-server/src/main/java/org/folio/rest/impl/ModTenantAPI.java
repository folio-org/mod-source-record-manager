package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.annotations.Validate;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.services.MappingRuleService;
import org.folio.services.util.ChangeManagerKafkaHandlers;
import org.folio.services.util.KafkaConfig;
import org.folio.services.util.PubSubConfig;
import org.folio.spring.SpringContextUtil;
import org.folio.util.pubsub.PubSubClientUtils;
import org.springframework.beans.factory.annotation.Autowired;

import javax.ws.rs.core.Response;
import java.util.Map;

import static java.lang.String.format;

public class ModTenantAPI extends TenantAPI {
  private static final Logger LOGGER = LoggerFactory.getLogger(ModTenantAPI.class);

  private static final String GRANT_SEQUENCES_PERMISSION_PATTERN = "GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA %s TO %s;";

  @Autowired
  private MappingRuleService mappingRuleService;

  @Autowired
  private KafkaConfig kafkaConfig;

  @Autowired
  private Vertx vertx;

  @Autowired
  private ChangeManagerKafkaHandlers changeManagerKafkaHandlers;

  public ModTenantAPI() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Validate
  @Override
  public void postTenant(TenantAttributes entity, Map<String, String> headers, Handler<AsyncResult<Response>> handler, Context context) {
    super.postTenant(entity, headers, postTenantAr -> {
      if (postTenantAr.failed()) {
        handler.handle(postTenantAr);
      } else {
        OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(headers, context.owner());
        String tenantId = TenantTool.calculateTenantId(okapiConnectionParams.getTenantId());
        setSequencesPermissionForDbUser(context, tenantId)
          .compose(ar -> mappingRuleService.saveDefaultRules(tenantId))
          .compose(ar -> registerModuleToPubsub(headers, context.owner()))
          .compose(ar -> subscribe("DI_INVENTORY_INSTANCE_CREATED", changeManagerKafkaHandlers.postChangeManagerHandlersCreatedInventoryInstance(tenantId), tenantId))
          .compose(ar -> subscribe("DI_COMPLETED", changeManagerKafkaHandlers.postChangeManagerHandlersProcessingResult(okapiConnectionParams), tenantId))
          .compose(ar -> subscribe("DI_ERROR", changeManagerKafkaHandlers.postChangeManagerHandlersProcessingResult(okapiConnectionParams), tenantId))
          .compose(ar -> subscribe("QM_INVENTORY_INSTANCE_UPDATED", changeManagerKafkaHandlers.postChangeManagerHandlersQmCompleted(tenantId), tenantId))
          .compose(ar -> subscribe("QM_ERROR", changeManagerKafkaHandlers.postChangeManagerHandlersQmError(tenantId), tenantId))
          .onComplete(event -> handler.handle(postTenantAr));
      }
    }, context);
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

  public Future<Boolean> subscribe(String eventName, Handler<KafkaConsumerRecord<String, String>> handler, String tenantId) {
    Promise<Boolean> promise = Promise.promise();
    String topicName = new PubSubConfig(kafkaConfig.getEnvId(), tenantId, eventName).getTopicName();
    Map<String, String> consumerProps = kafkaConfig.getConsumerProps();
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, topicName);
    KafkaConsumer.<String, String>create(vertx, consumerProps)
      .subscribe(topicName, ar -> {
        if (ar.succeeded()) {
          LOGGER.info(format("Subscribed to topic {%s}", topicName));
          promise.complete(true);
        } else {
          LOGGER.error(format("Could not subscribe to some of the topic {%s}", topicName), ar.cause());
          promise.fail(ar.cause());
        }
      }).handler(handler);
    return promise.future();
  }

}
