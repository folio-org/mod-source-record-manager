package org.folio.verticle;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.BackPressureGauge;
import org.folio.kafka.GlobalLoadSensor;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.ProcessRecordErrorHandler;
import org.folio.kafka.SubscriptionDefinition;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.verticle.consumers.consumerstorage.KafkaConsumersStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import java.util.ArrayList;
import java.util.List;

import static org.folio.services.util.EventHandlingUtil.constructModuleName;

public abstract class AbstractConsumersVerticle extends AbstractVerticle {
  private static final GlobalLoadSensor globalLoadSensor = new GlobalLoadSensor();

  @Autowired
  @Qualifier("newKafkaConfig")
  private KafkaConfig kafkaConfig;

  @Value("${srm.kafka.DataImportConsumer.loadLimit:5}")
  private int loadLimit;

  @Autowired
  private KafkaConsumersStorage kafkaConsumersStorage;

  @Value("${srm.kafka.DataImportConsumer.globalLoadLimit:35}")
  private int globalLoadLimit;

  @Override
  public void start(Promise<Void> startPromise) {
    List<Future<Void>> futures = new ArrayList<>();

    getEvents().forEach(event -> {
      SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper
        .createSubscriptionDefinition(kafkaConfig.getEnvId(),
          KafkaTopicNameHelper.getDefaultNameSpace(),
          event);

      KafkaConsumerWrapper<String, String> consumerWrapper = KafkaConsumerWrapper.<String, String>builder()
        .context(context)
        .vertx(vertx)
        .kafkaConfig(kafkaConfig)
        .loadLimit(loadLimit)
        .globalLoadSensor(globalLoadSensor)
        .addToGlobalLoad(shouldAddToGlobalLoad())
        .subscriptionDefinition(subscriptionDefinition)
        .processRecordErrorHandler(getErrorHandler())
        .backPressureGauge(getBackPressureGauge())
        .build();
      kafkaConsumersStorage.addConsumer(event, consumerWrapper);

      futures.add(consumerWrapper.start(getHandler(),
        constructModuleName() + "_" + getClass().getSimpleName()));
    });

    GenericCompositeFuture.all(futures).onComplete(ar -> startPromise.complete());
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    List<Future<Void>> futures = new ArrayList<>();
    kafkaConsumersStorage.getConsumersList().forEach(consumerWrapper ->
      futures.add(consumerWrapper.stop()));

    GenericCompositeFuture.join(futures).onComplete(ar -> stopPromise.complete());
  }

  public abstract List<String> getEvents();

  public abstract AsyncRecordHandler<String, String> getHandler();

  /**
   * Include messages from this consumer to mutate global load counts
   */
  public Boolean shouldAddToGlobalLoad() {
    return true;
  }

  /**
   * By default error handler is null and so not invoked by folio-kafka-wrapper for failure cases.
   * If you need to add error handling logic and send DI_ERROR events - override this method with own error handler
   * implementation for  particular consumer instance.
   *
   * @return error handler
   */
  public ProcessRecordErrorHandler<String, String> getErrorHandler() {
    return null;
  }

  /**
   * Implementation of function, that handles consuming load using kafka pause/resume methods.
   * If not specified - the default implementation from folio-kafka-wrapper will be used.
   *
   * @return back pressure gauge implementation
   */
  public BackPressureGauge<Integer, Integer, Integer> getBackPressureGauge() {
    return (g, l, t) -> (l > 0 && l > t) || (g > globalLoadLimit);
  }
}
