package org.folio.rest.impl;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.ThreadingModel;
import io.vertx.core.Vertx;
import io.vertx.core.spi.VerticleFactory;
import io.vertx.serviceproxy.ServiceBinder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.config.ApplicationConfig;
import org.folio.rest.resource.interfaces.InitAPI;
import org.folio.services.journal.JournalService;
import org.folio.services.journal.JournalUtil;
import org.folio.services.progress.JobExecutionProgressUtil;
import org.folio.spring.SpringContextUtil;
import org.folio.verticle.DataImportConsumersVerticle;
import org.folio.verticle.DataImportInitConsumersVerticle;
import org.folio.verticle.JobExecutionProgressVerticle;
import org.folio.verticle.DataImportJournalBatchConsumerVerticle;
import org.folio.verticle.RawMarcChunkConsumersVerticle;
import org.folio.verticle.StoredRecordChunkConsumersVerticle;
import org.folio.verticle.SpringVerticleFactory;
import org.folio.verticle.periodic.PeriodicDeleteJobExecutionVerticle;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractApplicationContext;

import java.util.List;

import static io.vertx.core.ThreadingModel.WORKER;

public class InitAPIImpl implements InitAPI {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final String SPRING_CONTEXT_KEY = "springContext";

  @Value("${srm.kafka.DataImportInitConsumersVerticle.instancesNumber:1}")
  private int initConsumerInstancesNumber;

  @Value("${srm.kafka.RawMarcChunkConsumer.instancesNumber:1}")
  private int rawMarcChunkConsumerInstancesNumber;

  @Value("${srm.kafka.StoredMarcChunkConsumer.instancesNumber:1}")
  private int storedMarcChunkConsumerInstancesNumber;

  @Value("${srm.kafka.DataImportConsumersVerticle.instancesNumber:1}")
  private int dataImportConsumerInstancesNumber;

  @Value("${srm.kafka.DataImportJournalConsumersVerticle.instancesNumber:1}")
  private int dataImportJournalConsumerInstancesNumber;

  @Value("${srm.kafka.JobExecutionProgressVerticle.instancesNumber:1}")
  private int jobExecutionProgressInstancesNumber;

  @Value("${srm.kafka.JobExecutionDeletion.instancesNumber:1}")
  private int jobExecutionDeletionInstanceNumber;

  @Autowired
  @Qualifier("journalService")
  private JournalService journalService;

  @Override
  public void init(Vertx vertx, Context context, Handler<AsyncResult<Boolean>> handler) {
    LOGGER.info("init:: InitAPI starting...");
    try {
      SpringContextUtil.init(vertx, context, ApplicationConfig.class);
      SpringContextUtil.autowireDependencies(this, context);

      JobExecutionProgressUtil.registerCodecs(vertx);
      JournalUtil.registerCodecs(vertx);

      initJournalService(vertx);
      deployConsumersVerticles(vertx)
        .onSuccess(car -> {
          handler.handle(Future.succeededFuture());
          LOGGER.info("init:: Consumer Verticles were successfully started");
        })
        .onFailure(th -> {
          handler.handle(Future.failedFuture(th));
          LOGGER.warn("init:: Consumer Verticles were not started", th);
        });
    } catch (Exception th) {
      LOGGER.warn("init:: Error during module init", th);
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

    return Future.all(List.of(
      deployWorkerVerticle(vertx, verticleFactory, DataImportInitConsumersVerticle.class, initConsumerInstancesNumber),
      deployWorkerVerticle(vertx, verticleFactory, RawMarcChunkConsumersVerticle.class, rawMarcChunkConsumerInstancesNumber),
      deployWorkerVerticle(vertx, verticleFactory, StoredRecordChunkConsumersVerticle.class, storedMarcChunkConsumerInstancesNumber),
      deployWorkerVerticle(vertx, verticleFactory, DataImportConsumersVerticle.class, dataImportConsumerInstancesNumber),
      deployWorkerVerticle(vertx, verticleFactory, DataImportJournalBatchConsumerVerticle.class, dataImportJournalConsumerInstancesNumber),
      deployWorkerVerticle(vertx, verticleFactory, JobExecutionProgressVerticle.class, jobExecutionProgressInstancesNumber),
      deployWorkerVerticle(vertx, verticleFactory, PeriodicDeleteJobExecutionVerticle.class, jobExecutionDeletionInstanceNumber)
    ));
  }

  private <T> String getVerticleName(VerticleFactory verticleFactory, Class<T> clazz) {
    return verticleFactory.prefix() + ":" + clazz.getName();
  }

  private Future<String> deployWorkerVerticle(Vertx vertx, VerticleFactory verticleFactory,
                                              Class<? extends AbstractVerticle> verticleClass, int instances) {
    DeploymentOptions deploymentOptions = new DeploymentOptions()
      .setThreadingModel(WORKER)
      .setInstances(instances);
    return vertx.deployVerticle(getVerticleName(verticleFactory, verticleClass), deploymentOptions);
  }
}
