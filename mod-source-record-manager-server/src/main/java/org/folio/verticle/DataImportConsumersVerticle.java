package org.folio.verticle;

import io.vertx.core.AbstractVerticle;
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
import java.util.Arrays;
import java.util.List;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_COMPLETED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;

public class DataImportConsumersVerticle extends AbstractVerticle {

  //TODO: get rid of this workaround with global spring context
  private static AbstractApplicationContext springGlobalContext;

  private static final GlobalLoadSensor globalLoadSensor = new GlobalLoadSensor();

  private final List<String> events = Arrays.asList(DI_COMPLETED.value(), DI_ERROR.value());

  @Autowired
  @Qualifier("DataImportKafkaHandler")
  private AsyncRecordHandler<String, String> dataImportKafkaHandler;

  @Autowired
  @Qualifier("newKafkaConfig")
  private KafkaConfig kafkaConfig;

  @Value("${srm.kafka.DataImportConsumer.loadLimit:5}")
  private int loadLimit;

  private List<KafkaConsumerWrapper<String, String>> consumerWrappersList = new ArrayList<>(events.size());

  @Override
  public void start(Promise<Void> startPromise) {
    context.put("springContext", springGlobalContext);

    SpringContextUtil.autowireDependencies(this, context);

    events.forEach(event -> {
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

    consumerWrappersList.forEach(consumerWrapper -> consumerWrapper
      .start(dataImportKafkaHandler, PubSubClientUtils.constructModuleName())
      .onComplete(ar -> startPromise.complete()));
  }

  @Override
  public void stop(Promise<Void> stopPromise) {
    consumerWrappersList.forEach(consumerWrapper -> consumerWrapper
      .stop()
      .onComplete(ar -> stopPromise.complete()));
  }

  //TODO: get rid of this workaround with global spring context
  @Deprecated
  public static void setSpringGlobalContext(AbstractApplicationContext springGlobalContext) {
    DataImportConsumersVerticle.springGlobalContext = springGlobalContext;
  }
}
