package org.folio.verticle;

import io.vertx.core.AbstractVerticle;
import org.folio.okapi.common.GenericCompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.GlobalLoadSensor;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.KafkaConsumerWrapper;
import org.folio.kafka.KafkaTopicNameHelper;
import org.folio.kafka.SubscriptionDefinition;
import org.folio.spring.SpringContextUtil;
import org.folio.util.pubsub.PubSubClientUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.support.AbstractApplicationContext;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractConsumersVerticle extends AbstractVerticle {

  //TODO: get rid of this workaround with global spring context
  private static AbstractApplicationContext springGlobalContext;

  private static final GlobalLoadSensor globalLoadSensor = new GlobalLoadSensor();

  @Autowired
  @Qualifier("newKafkaConfig")
  private KafkaConfig kafkaConfig;

  @Value("${srm.kafka.DataImportConsumer.loadLimit:5}")
  private int loadLimit;

  private List<KafkaConsumerWrapper<String, String>> consumerWrappersList = new ArrayList<>();

  @Override
  public void start(Promise<Void> startPromise) {
    context.put("springContext", springGlobalContext);

    SpringContextUtil.autowireDependencies(this, context);

    getEvents().forEach(event -> {
      SubscriptionDefinition subscriptionDefinition = KafkaTopicNameHelper
        .createSubscriptionDefinition(kafkaConfig.getEnvId(),
          KafkaTopicNameHelper.getDefaultNameSpace(),
          event);
      consumerWrappersList.add(KafkaConsumerWrapper.<String, String>builder()
        .context(context)
        .vertx(vertx)
        .kafkaConfig(kafkaConfig)
        .loadLimit(loadLimit)
        .globalLoadSensor(globalLoadSensor)
        .subscriptionDefinition(subscriptionDefinition)
        .build());
    });
    List<Future<Void>> futures = new ArrayList<>();
    consumerWrappersList.forEach(consumerWrapper ->
      futures.add(consumerWrapper.start(getHandler(), PubSubClientUtils.constructModuleName())));

    GenericCompositeFuture.all(futures).onComplete(ar -> startPromise.complete());
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    List<Future<Void>> futures = new ArrayList<>();
    consumerWrappersList.forEach(consumerWrapper ->
      futures.add(consumerWrapper.stop()));

    GenericCompositeFuture.join(futures).onComplete(ar -> stopPromise.complete());
  }

  //TODO: get rid of this workaround with global spring context
  @Deprecated
  public static void setSpringGlobalContext(AbstractApplicationContext springGlobalContext) {
    AbstractConsumersVerticle.springGlobalContext = springGlobalContext;
  }

  public abstract List<String> getEvents();

  public abstract AsyncRecordHandler<String, String> getHandler();

}
