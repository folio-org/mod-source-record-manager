package org.folio.config;

import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.kafka.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.folio.services.util.KafkaConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
@ComponentScan(basePackages = {
  "org.folio.rest.impl",
  "org.folio.dao",
  "org.folio.services",
  "org.folio.verticle.consumers"})
public class ApplicationConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationConfig.class);

  //TODO: get rid of old deprecated KafkaConfig
  @Bean
  public KafkaAdminClient kafkaAdminClient(@Autowired Vertx vertx, @Autowired KafkaConfig config) {
    Map<String, String> configs = new HashMap<>();
    configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaUrl());
    return KafkaAdminClient.create(vertx, configs);
  }

  //TODO: get rid of old deprecated KafkaConfig
  @Bean(name = "newKafkaConfig")
  public org.folio.kafka.KafkaConfig kafkaConfigBean(@Autowired KafkaConfig oldConfig) {
    org.folio.kafka.KafkaConfig kafkaConfig = org.folio.kafka.KafkaConfig.builder()
      .envId(oldConfig.getEnvId())
      .kafkaHost(oldConfig.getKafkaHost())
      .kafkaPort(oldConfig.getKafkaPort())
      .okapiUrl(oldConfig.getOkapiUrl())
      .replicationFactor(oldConfig.getReplicationFactor())
      .build();

    LOGGER.debug("kafkaConfig: " + kafkaConfig);

    return kafkaConfig;
  }
}
