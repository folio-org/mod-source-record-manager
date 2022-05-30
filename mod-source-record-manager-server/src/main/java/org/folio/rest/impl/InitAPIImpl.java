package org.folio.rest.impl;

import java.util.Arrays;

import io.vertx.core.AsyncResult;
import org.folio.okapi.common.GenericCompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.vertx.serviceproxy.ServiceBinder;
import org.folio.config.ApplicationConfig;
import org.folio.rest.resource.interfaces.InitAPI;
import org.folio.services.journal.JournalService;
import org.folio.spring.SpringContextUtil;
import org.folio.verticle.DataImportConsumersVerticle;
import org.folio.verticle.DataImportInitConsumersVerticle;
import org.folio.verticle.DataImportJournalConsumersVerticle;
import org.folio.verticle.JobMonitoringWatchdogVerticle;
import org.folio.verticle.QuickMarcUpdateConsumersVerticle;
import org.folio.verticle.RawMarcChunkConsumersVerticle;
import org.folio.verticle.StoredRecordChunkConsumersVerticle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

public class InitAPIImpl implements InitAPI {

  private static final Logger LOGGER = LogManager.getLogger();

  @Value("${srm.kafka.DataImportInitConsumersVerticle.instancesNumber:1}")
  private int initConsumerInstancesNumber;

  @Value("${srm.kafka.RawMarcChunkConsumer.instancesNumber:1}")
  private int rawMarcChunkConsumerInstancesNumber;

  // TODO: srm.kafka.StoredMarcChunkConsumer should be refactored
  @Value("${srm.kafka.StoredMarcChunkConsumer.instancesNumber:3}")
  private int storedMarcChunkConsumerInstancesNumber;

  @Value("${srm.kafka.DataImportConsumersVerticle.instancesNumber:3}")
  private int dataImportConsumerInstancesNumber;

  @Value("${srm.kafka.DataImportJournalConsumersVerticle.instancesNumber:3}")
  private int dataImportJournalConsumerInstancesNumber;

  @Value("${srm.kafka.QuickMarcUpdateConsumersVerticle.instancesNumber:1}")
  private int quickMarcUpdateConsumerInstancesNumber;

  @Value("${srm.kafka.JobMonitoringWatchdogVerticle.instancesNumber:1}")
  private int jobExecutionWatchdogInstanceNumber;

  @Autowired
  @Qualifier("journalService")
  private JournalService journalService;

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> handler) {
    LOGGER.info("InitAPI starting...");
    try {
      SpringContextUtil.init(vertx, context, ApplicationConfig.class);
      SpringContextUtil.autowireDependencies(this, context);
      initJournalService(vertx);
      deployConsumersVerticles(vertx)
        .onSuccess(car -> {
          handler.handle(Future.succeededFuture());
          LOGGER.info("Consumer Verticles were successfully started");
        })
        .onFailure(th -> {
          handler.handle(Future.failedFuture(th));
          LOGGER.error("Consumer Verticles were not started", th);
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

  private Future<?> deployConsumersVerticles(Vertx vertx) {
    //TODO: get rid of this workaround with global spring context
    DataImportInitConsumersVerticle.setSpringGlobalContext(vertx.getOrCreateContext().get("springContext"));
    RawMarcChunkConsumersVerticle.setSpringGlobalContext(vertx.getOrCreateContext().get("springContext"));
    StoredRecordChunkConsumersVerticle.setSpringGlobalContext(vertx.getOrCreateContext().get("springContext"));
    DataImportConsumersVerticle.setSpringGlobalContext(vertx.getOrCreateContext().get("springContext"));
    DataImportJournalConsumersVerticle.setSpringGlobalContext(vertx.getOrCreateContext().get("springContext"));
    QuickMarcUpdateConsumersVerticle.setSpringGlobalContext(vertx.getOrCreateContext().get("springContext"));
    JobMonitoringWatchdogVerticle.setSpringContext(vertx.getOrCreateContext().get("springContext"));

    Promise<String> deployInitConsumer = Promise.promise();
    Promise<String> deployRawMarcChunkConsumer = Promise.promise();
    Promise<String> deployStoredMarcChunkConsumer = Promise.promise();
    Promise<String> deployDataImportConsumer = Promise.promise();
    Promise<String> deployDataImportJournalConsumer = Promise.promise();
    Promise<String> deployQuickMarcUpdateConsumer = Promise.promise();
    Promise<String> deployJobExecutionWatchdog = Promise.promise();

    vertx.deployVerticle("org.folio.verticle.DataImportInitConsumersVerticle",
      new DeploymentOptions()
        .setWorker(true)
        .setInstances(initConsumerInstancesNumber), deployInitConsumer);

    vertx.deployVerticle("org.folio.verticle.RawMarcChunkConsumersVerticle",
      new DeploymentOptions()
        .setWorker(true)
        .setInstances(rawMarcChunkConsumerInstancesNumber), deployRawMarcChunkConsumer);

    vertx.deployVerticle("org.folio.verticle.StoredRecordChunkConsumersVerticle",
      new DeploymentOptions()
        .setWorker(true)
        .setInstances(storedMarcChunkConsumerInstancesNumber), deployStoredMarcChunkConsumer);

    vertx.deployVerticle("org.folio.verticle.DataImportConsumersVerticle",
      new DeploymentOptions()
        .setWorker(true)
        .setInstances(dataImportConsumerInstancesNumber), deployDataImportConsumer);

    vertx.deployVerticle("org.folio.verticle.DataImportJournalConsumersVerticle",
      new DeploymentOptions()
        .setWorker(true)
        .setInstances(dataImportJournalConsumerInstancesNumber), deployDataImportJournalConsumer);

    vertx.deployVerticle("org.folio.verticle.QuickMarcUpdateConsumersVerticle",
      new DeploymentOptions()
        .setWorker(true)
        .setInstances(quickMarcUpdateConsumerInstancesNumber), deployQuickMarcUpdateConsumer);

    vertx.deployVerticle("org.folio.verticle.JobMonitoringWatchdogVerticle",
      new DeploymentOptions()
        .setWorker(true)
        .setInstances(jobExecutionWatchdogInstanceNumber), deployJobExecutionWatchdog);

    return GenericCompositeFuture.all(Arrays.asList(
      deployInitConsumer.future(),
      deployRawMarcChunkConsumer.future(),
      deployStoredMarcChunkConsumer.future(),
      deployDataImportConsumer.future(),
      deployDataImportJournalConsumer.future(),
      deployQuickMarcUpdateConsumer.future(),
      deployJobExecutionWatchdog.future()));
  }
}
