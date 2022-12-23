package org.folio.services;

import static io.vertx.core.Future.failedFuture;
import static java.lang.String.format;

import static org.folio.HttpStatus.HTTP_NOT_FOUND;
import static org.folio.HttpStatus.HTTP_OK;
import static org.folio.verticle.consumers.util.QMEventTypes.QM_RECORD_UPDATED;

import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import javax.ws.rs.NotFoundException;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import lombok.extern.log4j.Log4j2;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.Try;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.client.SourceStorageSourceRecordsClient;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordState;
import org.folio.services.entity.MappingRuleCacheKey;
import org.folio.services.mappers.processor.MappingParametersProvider;

@Log4j2
@Service
public class ParsedRecordServiceImpl implements ParsedRecordService {
  private static final Logger LOGGER = LogManager.getLogger();
  private static final AtomicInteger indexer = new AtomicInteger();

  private final MappingParametersProvider mappingParametersProvider;
  private final MappingRuleCache mappingRuleCache;
  private final SourceRecordStateService sourceRecordStateService;
  private final QuickMarcEventProducerService producerService;

  @Value("${srm.kafka.QuickMarcUpdateKafkaHandler.maxDistributionNum:100}")
  private int maxDistributionNum;

  public ParsedRecordServiceImpl(MappingParametersProvider mappingParametersProvider,
                                 MappingRuleCache mappingRuleCache,
                                 SourceRecordStateService sourceRecordStateService, QuickMarcEventProducerService producerService) {
    this.mappingParametersProvider = mappingParametersProvider;
    this.mappingRuleCache = mappingRuleCache;
    this.sourceRecordStateService = sourceRecordStateService;
    this.producerService = producerService;
  }

  @Override
  public Future<ParsedRecordDto> getRecordByExternalId(String externalId, OkapiConnectionParams params) {
    LOGGER.debug("getRecordByExternalId:: externalId {}", externalId);
    Promise<ParsedRecordDto> promise = Promise.promise();
    var client = new SourceStorageSourceRecordsClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.getSourceStorageSourceRecordsById(externalId, "EXTERNAL", response -> {
        if (HTTP_OK.toInt() == response.result().statusCode()) {
          Buffer bodyAsBuffer = response.result().bodyAsBuffer();
          Try.itGet(() -> mapSourceRecordToParsedRecordDto(bodyAsBuffer))
            .compose(parsedRecordDto -> sourceRecordStateService.get(parsedRecordDto.getId(), params.getTenantId())
              .map(sourceRecordStateOptional -> sourceRecordStateOptional
                .orElse(new SourceRecordState().withRecordState(SourceRecordState.RecordState.ACTUAL)))
              .compose(sourceRecordState -> Future.succeededFuture(parsedRecordDto
                .withRecordState(ParsedRecordDto.RecordState.valueOf(sourceRecordState.getRecordState().name())))))
            .onComplete(parsedRecordDtoAsyncResult -> {
              if (parsedRecordDtoAsyncResult.succeeded()) {
                promise.complete(parsedRecordDtoAsyncResult.result());
              } else {
                promise.fail(parsedRecordDtoAsyncResult.cause());
              }
            });
        } else {
          String message = format("Error retrieving Record by externalId: '%s', response code %s, %s",
            externalId, response.result().statusCode(), response.result().statusMessage());
          if (HTTP_NOT_FOUND.toInt() == response.result().statusCode()) {
            promise.fail(new NotFoundException(message));
          } else {
            promise.fail(message);
          }
        }
      });
    } catch (Exception e) {
      log.warn("getRecordByExternalId:: Failed to GET Record from SRS", e);
      promise.fail(e);
    }
    return promise.future();
  }

  @Override
  public Future<Boolean> updateRecord(ParsedRecordDto parsedRecordDto, OkapiConnectionParams params) {
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
    var sourceRecordState = new SourceRecordState()
      .withRecordState(SourceRecordState.RecordState.IN_PROGRESS)
      .withSourceRecordId(parsedRecordDto.getId());
    var tenantId = params.getTenantId();
    var key = String.valueOf(indexer.incrementAndGet() % maxDistributionNum);
    var kafkaHeaders = KafkaHeaderUtils.kafkaHeadersFromMultiMap(params.getHeaders());
    var eventPayload = prepareEventPayload(parsedRecordDto, mappingRules, mappingParameters, snapshotId);
    return sourceRecordStateService.save(sourceRecordState, tenantId)
      .compose(s -> producerService.sendEventWithZipping(eventPayload, QM_RECORD_UPDATED.name(), key, tenantId, kafkaHeaders));
  }

  private ParsedRecordDto mapSourceRecordToParsedRecordDto(Buffer body) {
    SourceRecord sourceRecord = body.toJsonObject().mapTo(SourceRecord.class);
    return new ParsedRecordDto()
      .withId(sourceRecord.getRecordId())
      .withParsedRecord(sourceRecord.getParsedRecord())
      .withRecordType(ParsedRecordDto.RecordType.fromValue(sourceRecord.getRecordType().value()))
      .withExternalIdsHolder(sourceRecord.getExternalIdsHolder())
      .withAdditionalInfo(sourceRecord.getAdditionalInfo())
      .withMetadata(sourceRecord.getMetadata())
      .withRecordState(ParsedRecordDto.RecordState.ACTUAL);
  }

  private String prepareEventPayload(ParsedRecordDto parsedRecordDto, JsonObject mappingRules,
                                     MappingParameters mappingParameters, String snapshotId) {
    HashMap<String, String> eventPayload = new HashMap<>();
    eventPayload.put("PARSED_RECORD_DTO", Json.encode(parsedRecordDto));
    eventPayload.put("MAPPING_RULES", mappingRules.encode());
    eventPayload.put("MAPPING_PARAMS", Json.encode(mappingParameters));
    eventPayload.put("SNAPSHOT_ID", snapshotId);
    eventPayload.put("RECORD_ID", parsedRecordDto.getParsedRecord().getId());
    eventPayload.put("RECORD_DTO_ID", parsedRecordDto.getId());
    eventPayload.put("RECORD_TYPE", parsedRecordDto.getRecordType().value());
    return Json.encode(eventPayload);
  }

}
