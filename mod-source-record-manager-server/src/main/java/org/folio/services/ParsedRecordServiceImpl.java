package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.Try;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.client.SourceStorageSourceRecordsClient;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.folio.rest.jaxrs.model.SourceRecordState;
import org.folio.services.mappers.processor.MappingParametersProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.ws.rs.NotFoundException;
import java.util.HashMap;
import java.util.UUID;

import static java.lang.String.format;
import static org.folio.HttpStatus.HTTP_NOT_FOUND;
import static org.folio.HttpStatus.HTTP_OK;
import static org.folio.services.util.EventHandlingUtil.sendEventWithPayloadToPubSub;

@Service
public class ParsedRecordServiceImpl implements ParsedRecordService {

  private static final Logger LOGGER = LogManager.getLogger();

  private static final String QM_RECORD_UPDATED_EVENT_TYPE = "QM_RECORD_UPDATED";

  private MappingParametersProvider mappingParametersProvider;
  private MappingRuleCache mappingRuleCache;
  private SourceRecordStateService sourceRecordStateService;

  public ParsedRecordServiceImpl(@Autowired MappingParametersProvider mappingParametersProvider,
                                 @Autowired MappingRuleCache mappingRuleCache,
                                 @Autowired SourceRecordStateService sourceRecordStateService
  ) {
    this.mappingParametersProvider = mappingParametersProvider;
    this.mappingRuleCache = mappingRuleCache;
    this.sourceRecordStateService = sourceRecordStateService;
  }

  @Override
  public Future<ParsedRecordDto> getRecordByInstanceId(String instanceId, OkapiConnectionParams params) {
    Promise<ParsedRecordDto> promise = Promise.promise();
    SourceStorageSourceRecordsClient client = new SourceStorageSourceRecordsClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.getSourceStorageSourceRecordsById(instanceId, "INSTANCE", response -> {
        if (HTTP_OK.toInt() == response.result().statusCode()) {
          Buffer bodyAsBuffer = response.result().bodyAsBuffer();
          Try.itGet(() -> mapSourceRecordToParsedRecordDto(bodyAsBuffer))
            .compose(parsedRecordDto -> sourceRecordStateService.get(parsedRecordDto.getId(), params.getTenantId())
              .map(sourceRecordStateOptional -> sourceRecordStateOptional.orElse(new SourceRecordState().withRecordState(SourceRecordState.RecordState.ACTUAL)))
              .compose(sourceRecordState -> Future.succeededFuture(parsedRecordDto.withRecordState(ParsedRecordDto.RecordState.valueOf(sourceRecordState.getRecordState().name())))))
            .onComplete(parsedRecordDtoAsyncResult -> {
              if (parsedRecordDtoAsyncResult.succeeded()) {
                promise.complete(parsedRecordDtoAsyncResult.result());
              } else {
                promise.fail(parsedRecordDtoAsyncResult.cause());
              }
            });
        } else {
          String message = format("Error retrieving Record by instanceId: '%s', response code %s, %s",
            instanceId, response.result().statusCode(), response.result().statusMessage());
          if (HTTP_NOT_FOUND.toInt() == response.result().statusCode()) {
            promise.fail(new NotFoundException(message));
          } else {
            promise.fail(message);
          }
        }
      });
    } catch (Exception e) {
      LOGGER.error("Failed to GET Record from SRS", e);
      promise.fail(e);
    }
    return promise.future();
  }

  @Override
  public Future<Boolean> updateRecord(ParsedRecordDto parsedRecordDto, OkapiConnectionParams params) {
    String snapshotId = UUID.randomUUID().toString();
    return mappingParametersProvider.get(snapshotId, params)
      .compose(mappingParameters -> mappingRuleCache.get(params.getTenantId())
        .compose(rulesOptional -> {
          if (rulesOptional.isPresent()) {
            SourceRecordState sourceRecordState = new SourceRecordState()
              .withRecordState(SourceRecordState.RecordState.IN_PROGRESS)
              .withSourceRecordId(parsedRecordDto.getId());
            return sourceRecordStateService.save(sourceRecordState, params.getTenantId())
              .compose(s -> sendEventWithPayloadToPubSub(prepareEventPayload(parsedRecordDto, rulesOptional.get(), mappingParameters, snapshotId),
                QM_RECORD_UPDATED_EVENT_TYPE, params));
          } else {
            return Future.failedFuture(format("Can not send %s event, no mapping rules found for tenant %s", QM_RECORD_UPDATED_EVENT_TYPE, params.getTenantId()));
          }
        }));
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
    eventPayload.put("RECORD_ID", parsedRecordDto.getId());

    return Json.encode(eventPayload);
  }

}
