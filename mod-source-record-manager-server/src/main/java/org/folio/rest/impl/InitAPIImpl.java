package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.serviceproxy.ServiceBinder;
import org.folio.config.ApplicationConfig;
import org.folio.rest.resource.interfaces.InitAPI;
import org.folio.services.journal.JournalService;
import org.folio.spring.SpringContextUtil;
import org.folio.verticle.consumers.RawMarcChunkConsumersVerticle;
import org.springframework.beans.factory.annotation.Value;

public class InitAPIImpl implements InitAPI {

  @Value("${srm.kafka.RawMarcChunkConsumer.instancesNumber:10}")
  private int rawMarcChunkConsumerInstancesNumber;

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> handler) {

    vertx.executeBlocking(
      future -> {
        SpringContextUtil.init(vertx, context, ApplicationConfig.class);
        SpringContextUtil.autowireDependencies(this, context);
        future.complete();
      },
      result -> {
        if (result.succeeded()) {
          initJournalService(vertx);
          deployRawMarcChunkConsumersVerticles(vertx).onComplete(car -> {
            if (car.succeeded()) {
              handler.handle(Future.succeededFuture(true));
            } else {
              handler.handle(Future.failedFuture(car.cause()));
            }
          });
        } else {
          handler.handle(Future.failedFuture(result.cause()));
        }
      });
  }

  private void initJournalService(Vertx vertx) {
    new ServiceBinder(vertx)
      .setAddress(JournalService.JOURNAL_RECORD_SERVICE_ADDRESS)
      .register(JournalService.class, JournalService.create());
  }

  private Future<String> deployRawMarcChunkConsumersVerticles(Vertx vertx) {
    //TODO: get rid of this workaround with global spring context
    RawMarcChunkConsumersVerticle.setSpringGlobalContext(vertx.getOrCreateContext().get("springContext"));

    Promise<String> deployConsumers = Promise.promise();

    vertx.deployVerticle("org.folio.verticle.consumers.RawMarcChunkConsumersVerticle",
      new DeploymentOptions().setWorker(true).setInstances(rawMarcChunkConsumerInstancesNumber), deployConsumers);

    return deployConsumers.future();
  }
}
