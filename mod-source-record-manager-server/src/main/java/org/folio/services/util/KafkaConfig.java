package org.folio.services.util;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Component
public class KafkaConfig {

  @Value("${KAFKA_HOST:10.0.2.15}")
  private String kafkaHost;
  @Value("${KAFKA_PORT:9092}")
  private String kafkaPort;
  @Value("${OKAPI_URL:http://10.0.2.15:9130}")
  private String okapiUrl;
  @Value("${REPLICATION_FACTOR:1}")
  private int replicationFactor;
  @Value("${NUMBER_OF_PARTITIONS:1}")
  private int numberOfPartitions;
  @Value("${ENV:folio}")
  private String envId;

  public String getKafkaHost() {
    return kafkaHost;
  }

  public String getKafkaPort() {
    return kafkaPort;
  }

  public String getOkapiUrl() {
    return okapiUrl;
  }

  public Map<String, String> getProducerProps() {
    Map<String, String> producerProps = new HashMap<>();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaUrl());
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
    return producerProps;
  }

  public Map<String, String> getConsumerProps() {
    Map<String, String> consumerProps = new HashMap<>();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaUrl());
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    consumerProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    return consumerProps;
  }

  public String getKafkaUrl() {
    return kafkaHost + ":" + kafkaPort;
  }

  public int getReplicationFactor() {
    return replicationFactor;
  }

  public int getNumberOfPartitions() {
    return numberOfPartitions;
  }

  public String getEnvId() {
    return envId;
  }
}
