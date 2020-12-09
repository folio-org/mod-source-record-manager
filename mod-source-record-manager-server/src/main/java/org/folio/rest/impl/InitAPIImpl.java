package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.serviceproxy.ServiceBinder;
import org.folio.config.ApplicationConfig;
import org.folio.rest.resource.interfaces.InitAPI;
import org.folio.services.journal.JournalService;
import org.folio.spring.SpringContextUtil;
import org.folio.verticle.consumers.RawMarcChunkConsumersVerticle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

public class InitAPIImpl implements InitAPI {

  private static final Logger LOGGER = LoggerFactory.getLogger(ModTenantAPI.class);

  @Value("${srm.kafka.RawMarcChunkConsumer.instancesNumber:5}")
  private int rawMarcChunkConsumerInstancesNumber;

  @Autowired
  @Qualifier("journalService")
  private JournalService journalService;

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> handler) {
    try {
      SpringContextUtil.init(vertx, context, ApplicationConfig.class);
      SpringContextUtil.autowireDependencies(this, context);
      initJournalService(vertx);
      deployRawMarcChunkConsumersVerticles(vertx)
        .onComplete(car -> {
          handler.handle(Future.succeededFuture());
          LOGGER.info("RawMarcChunkConsumers Verticles successfully started");
        })
        .onFailure(th -> {
          handler.handle(Future.failedFuture(th));
          LOGGER.error("RawMarcChunkConsumers Verticles were not started", th);
        });
    } catch (Throwable th) {
      LOGGER.error("Error during module init", th);
      handler.handle(Future.failedFuture(th));
    }
  }

  private void initJournalService(Vertx vertx) {
    new ServiceBinder(vertx)
      .setAddress(JournalService.JOURNAL_RECORD_SERVICE_ADDRESS)
      .register(JournalService.class, journalService);
  }

  private Future<?> deployRawMarcChunkConsumersVerticles(Vertx vertx) {
    //TODO: get rid of this workaround with global spring context
    RawMarcChunkConsumersVerticle.setSpringGlobalContext(vertx.getOrCreateContext().get("springContext"));

    Promise<String> deployRawMarcChunkConsumer = Promise.promise();

    vertx.deployVerticle("org.folio.verticle.consumers.RawMarcChunkConsumersVerticle",
      new DeploymentOptions()
        .setWorker(true)
        .setInstances(rawMarcChunkConsumerInstancesNumber),
      deployRawMarcChunkConsumer);

    return deployRawMarcChunkConsumer.future();
  }
}
