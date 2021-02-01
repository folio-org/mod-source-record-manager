package org.folio.config;

import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.kafka.KafkaConfig;
import org.folio.services.journal.JournalService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {
  "org.folio.rest.impl",
  "org.folio.dao",
  "org.folio.services",
  "org.folio.verticle"})
public class ApplicationConfig {
  private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationConfig.class);

  @Value("${FOLIO_KAFKA_HOST:kafka}")
  private String kafkaHost;
  @Value("${FOLIO_KAFKA_PORT:9092}")
  private String kafkaPort;
  @Value("${OKAPI_URL:http://okapi:9130}")
  private String okapiUrl;
  @Value("${FOLIO_KAFKA_REPLICATION_FACTOR:1}")
  private int replicationFactor;
  @Value("${FOLIO_KAFKA_ENV:folio}")
  private String envId;

  @Bean(name = "newKafkaConfig")
  public KafkaConfig kafkaConfigBean() {
    KafkaConfig kafkaConfig = KafkaConfig.builder()
      .envId(envId)
      .kafkaHost(kafkaHost)
      .kafkaPort(kafkaPort)
      .okapiUrl(okapiUrl)
      .replicationFactor(replicationFactor)
      .build();

    LOGGER.debug("kafkaConfig: " + kafkaConfig);

    return kafkaConfig;
  }

  @Autowired
  private Vertx vertx;

  @Bean(value = "journalServiceProxy")
  public JournalService journalServiceProxy() {
    return JournalService.createProxy(vertx);
  }
}
