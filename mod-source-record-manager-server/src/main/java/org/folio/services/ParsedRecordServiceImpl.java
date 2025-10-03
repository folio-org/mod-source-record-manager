package org.folio.services;

import static io.vertx.core.Future.failedFuture;
import static java.lang.String.format;

import static org.folio.verticle.consumers.util.QMEventTypes.QM_RECORD_UPDATED;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.services.entity.MappingRuleCacheKey;
import org.folio.services.mappers.processor.MappingParametersProvider;

@Log4j2
@Service
public class ParsedRecordServiceImpl implements ParsedRecordService {
  private static final AtomicInteger indexer = new AtomicInteger();

  private final MappingParametersProvider mappingParametersProvider;
  private final MappingRuleCache mappingRuleCache;
  private final QuickMarcEventProducerService producerService;

  @Value("${srm.kafka.QuickMarcUpdateKafkaHandler.maxDistributionNum:100}")
  private int maxDistributionNum;

  public ParsedRecordServiceImpl(MappingParametersProvider mappingParametersProvider,
                                 MappingRuleCache mappingRuleCache,
                                 QuickMarcEventProducerService producerService) {
    this.mappingParametersProvider = mappingParametersProvider;
    this.mappingRuleCache = mappingRuleCache;
    this.producerService = producerService;
  }

  @Override
  public Future<Boolean> updateRecord(ParsedRecordDto parsedRecordDto, OkapiConnectionParams params) {
    log.info("PARSED RECORD: {}", parsedRecordDto);
    String snapshotId = UUID.randomUUID().toString();
    MappingRuleCacheKey cacheKey = new MappingRuleCacheKey(params.getTenantId(), parsedRecordDto.getRecordType());
    return mappingParametersProvider.get(snapshotId, params)
      .compose(mappingParameters -> mappingRuleCache.get(cacheKey)
        .compose(rulesOptional -> {
          if (rulesOptional.isPresent()) {
            return updateRecord(parsedRecordDto, snapshotId, mappingParameters, rulesOptional.get(), params);
          } else {
            var message = format("Can not send %s event, no mapping rules found for tenant %s", QM_RECORD_UPDATED.name(),
              params.getTenantId());
            log.warn(message);
            return failedFuture(message);
          }
        }));
  }

  private Future<Boolean> updateRecord(ParsedRecordDto parsedRecordDto, String snapshotId,
                                       MappingParameters mappingParameters, JsonObject mappingRules,
                                       OkapiConnectionParams params) {
    var tenantId = params.getTenantId();
    var key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);
    var kafkaHeaders = KafkaHeaderUtils.kafkaHeadersFromMultiMap(params.getHeaders());
    var eventPayload = prepareEventPayload(parsedRecordDto, mappingRules, mappingParameters, snapshotId);
    return producerService.sendEventWithZipping(eventPayload, QM_RECORD_UPDATED.name(), key, tenantId, kafkaHeaders);
  }

  private String prepareEventPayload(ParsedRecordDto parsedRecordDto, JsonObject mappingRules,
                                     MappingParameters mappingParameters, String snapshotId) {
    HashMap<String, Object> eventPayload = new HashMap<>();
    eventPayload.put("PARSED_RECORD_DTO", parsedRecordDto);
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", mappingParameters);
    eventPayload.put("SNAPSHOT_ID", snapshotId);
    eventPayload.put("RECORD_ID", parsedRecordDto.getParsedRecord().getId());
    eventPayload.put("RECORD_DTO_ID", parsedRecordDto.getId());
    eventPayload.put("RECORD_TYPE", parsedRecordDto.getRecordType().value());
    var encode = Json.encode(eventPayload);
    log.info("QM_RECORD_UPDATED payload: {}", encode);
    return encode;
  }

}
