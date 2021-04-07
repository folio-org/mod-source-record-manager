package org.folio.config;

import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.marc.MarcRecordAnalyzer;
import org.folio.kafka.KafkaConfig;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.kafka.cache.util.CacheUtil;
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
  private static final Logger LOGGER = LogManager.getLogger();

  @Value("${KAFKA_HOST:kafka}")
  private String kafkaHost;
  @Value("${KAFKA_PORT:9092}")
  private String kafkaPort;
  @Value("${OKAPI_URL:http://okapi:9130}")
  private String okapiUrl;
  @Value("${REPLICATION_FACTOR:1}")
  private int replicationFactor;
  @Value("${ENV:folio}")
  private String envId;
  @Value("${srm.kafkacache.cleanup.interval.ms:3600000}")
  private long cacheCleanupIntervalMillis;
  @Value("${srm.kafkacache.expiration.time.hours:3}")
  private int cachedEventExpirationTimeHours;

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

  @Bean
  public KafkaInternalCache kafkaInternalCache(KafkaConfig kafkaConfig) {
    KafkaInternalCache kafkaInternalCache = KafkaInternalCache.builder()
      .kafkaConfig(kafkaConfig)
      .build();

    kafkaInternalCache.initKafkaCache();
    CacheUtil.initCacheCleanupPeriodicTask(vertx, kafkaInternalCache, cacheCleanupIntervalMillis, cachedEventExpirationTimeHours);

    return kafkaInternalCache;
  }
}
