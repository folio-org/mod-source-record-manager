package org.folio.config;

import io.vertx.core.Vertx;
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
  "org.folio.services"})
public class ApplicationConfig {

  @Bean
  public KafkaAdminClient kafkaAdminClient(@Autowired Vertx vertx, @Autowired KafkaConfig config) {
    Map<String, String> configs = new HashMap<>();
    configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaUrl());
    return KafkaAdminClient.create(vertx, configs);
  }
}
