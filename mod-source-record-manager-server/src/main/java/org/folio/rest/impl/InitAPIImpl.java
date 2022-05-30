package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.spi.VerticleFactory;
import io.vertx.serviceproxy.ServiceBinder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.config.ApplicationConfig;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.rest.resource.interfaces.InitAPI;
import org.folio.services.journal.JournalService;
import org.folio.spring.SpringContextUtil;
import org.folio.verticle.DataImportConsumersVerticle;
import org.folio.verticle.DataImportInitConsumersVerticle;
import org.folio.verticle.DataImportJournalConsumersVerticle;
import org.folio.verticle.QuickMarcUpdateConsumersVerticle;
import org.folio.verticle.RawMarcChunkConsumersVerticle;
import org.folio.verticle.StoredRecordChunkConsumersVerticle;
import org.folio.verticle.SpringVerticleFactory;
import org.folio.verticle.periodic.PeriodicDeleteJobExecutionVerticle;
import org.folio.verticle.periodic.PeriodicJobMonitoringWatchdogVerticle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractApplicationContext;

import java.util.Arrays;

public class InitAPIImpl implements InitAPI {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final String SPRING_CONTEXT_KEY = "springContext";

  @Value("${srm.kafka.DataImportInitConsumersVerticle.instancesNumber:1}")
  private int initConsumerInstancesNumber;

  @Value("${srm.kafka.RawMarcChunkConsumer.instancesNumber:1}")
  private int rawMarcChunkConsumerInstancesNumber;

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

  @Value("${srm.kafka.JobExecutionDeletion.instancesNumber:1}")
  private int jobExecutionDeletionInstanceNumber;

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
    AbstractApplicationContext springContext = vertx.getOrCreateContext().get(SPRING_CONTEXT_KEY);
    VerticleFactory verticleFactory = springContext.getBean(SpringVerticleFactory.class);
    vertx.registerVerticleFactory(verticleFactory);

    Promise<String> deployInitConsumer = Promise.promise();
    Promise<String> deployRawMarcChunkConsumer = Promise.promise();
    Promise<String> deployStoredMarcChunkConsumer = Promise.promise();
    Promise<String> deployDataImportConsumer = Promise.promise();
    Promise<String> deployDataImportJournalConsumer = Promise.promise();
    Promise<String> deployQuickMarcUpdateConsumer = Promise.promise();
    Promise<String> deployPeriodicJobExecutionWatchdog = Promise.promise();
    Promise<String> deployPeriodicDeleteJobExecution = Promise.promise();

    vertx.deployVerticle(verticleFactory.prefix() + ":" + DataImportInitConsumersVerticle.class.getName(),
      new DeploymentOptions()
        .setWorker(true)
        .setInstances(initConsumerInstancesNumber), deployInitConsumer);

    vertx.deployVerticle(verticleFactory.prefix() + ":" + RawMarcChunkConsumersVerticle.class.getName(),
      new DeploymentOptions()
        .setWorker(true)
        .setInstances(rawMarcChunkConsumerInstancesNumber), deployRawMarcChunkConsumer);

    vertx.deployVerticle(verticleFactory.prefix() + ":" + StoredRecordChunkConsumersVerticle.class.getName(),
      new DeploymentOptions()
        .setWorker(true)
        .setInstances(storedMarcChunkConsumerInstancesNumber), deployStoredMarcChunkConsumer);

    vertx.deployVerticle(verticleFactory.prefix() + ":" + DataImportConsumersVerticle.class.getName(),
      new DeploymentOptions()
        .setWorker(true)
        .setInstances(dataImportConsumerInstancesNumber), deployDataImportConsumer);

    vertx.deployVerticle(verticleFactory.prefix() + ":" + DataImportJournalConsumersVerticle.class.getName(),
      new DeploymentOptions()
        .setWorker(true)
        .setInstances(dataImportJournalConsumerInstancesNumber), deployDataImportJournalConsumer);

    vertx.deployVerticle(verticleFactory.prefix() + ":" + QuickMarcUpdateConsumersVerticle.class.getName(),
      new DeploymentOptions()
        .setWorker(true)
        .setInstances(quickMarcUpdateConsumerInstancesNumber), deployQuickMarcUpdateConsumer);

    vertx.deployVerticle(verticleFactory.prefix() + ":" + PeriodicJobMonitoringWatchdogVerticle.class.getName(),
      new DeploymentOptions()
        .setWorker(true)
        .setInstances(jobExecutionWatchdogInstanceNumber), deployPeriodicJobExecutionWatchdog);

    vertx.deployVerticle(verticleFactory.prefix() + ":" + PeriodicDeleteJobExecutionVerticle.class.getName(),
      new DeploymentOptions()
        .setWorker(true)
        .setInstances(jobExecutionDeletionInstanceNumber), deployPeriodicDeleteJobExecution);

    return GenericCompositeFuture.all(Arrays.asList(
      deployInitConsumer.future(),
      deployRawMarcChunkConsumer.future(),
      deployStoredMarcChunkConsumer.future(),
      deployDataImportConsumer.future(),
      deployDataImportJournalConsumer.future(),
      deployQuickMarcUpdateConsumer.future(),
      deployPeriodicDeleteJobExecution.future(),
      deployPeriodicJobExecutionWatchdog.future()));
  }
}
