package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
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

  @Value("${srm.kafka.RawMarcChunkConsumer.instancesNumber:5}")
  private int rawMarcChunkConsumerInstancesNumber;

  @Value("${srm.kafka.StoredMarcChunkConsumer.instancesNumber:5}")
  private int storedMarcChunkConsumerInstancesNumber;

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> handler) {
    try {
      SpringContextUtil.init(vertx, context, ApplicationConfig.class);
      SpringContextUtil.autowireDependencies(this, context);
      initJournalService(vertx);
      deployRawMarcChunkConsumersVerticles(vertx).onComplete(car -> {
        if (car.succeeded()) {
          handler.handle(Future.succeededFuture(true));
        } else {
          handler.handle(Future.failedFuture(car.cause()));
        }
      });
    } catch (Throwable th) {
      th.printStackTrace();
      handler.handle(Future.failedFuture(th));
    }
  }

  private void initJournalService(Vertx vertx) {
    new ServiceBinder(vertx)
      .setAddress(JournalService.JOURNAL_RECORD_SERVICE_ADDRESS)
      .register(JournalService.class, JournalService.create());
  }

  private Future<?> deployRawMarcChunkConsumersVerticles(Vertx vertx) {
    //TODO: get rid of this workaround with global spring context
    RawMarcChunkConsumersVerticle.setSpringGlobalContext(vertx.getOrCreateContext().get("springContext"));

    Promise<String> deployConsumers1 = Promise.promise();
    Promise<String> deployConsumers2 = Promise.promise();

    vertx.deployVerticle("org.folio.verticle.consumers.RawMarcChunkConsumersVerticle",
      new DeploymentOptions().setWorker(true).setInstances(rawMarcChunkConsumerInstancesNumber), deployConsumers1);

    vertx.deployVerticle("org.folio.verticle.consumers.StoredMarcChunkConsumersVerticle",
      new DeploymentOptions().setWorker(true).setInstances(storedMarcChunkConsumerInstancesNumber), deployConsumers2);

    return CompositeFuture.all(deployConsumers1.future(), deployConsumers2.future());
  }
}
