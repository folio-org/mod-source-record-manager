package org.folio.services;

import org.folio.HttpStatus;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.dataimport.util.Try;
import org.folio.rest.client.SourceStorageClient;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.SourceRecord;
import org.springframework.stereotype.Service;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

@Service
public class ParsedRecordServiceImpl implements ParsedRecordService {

  private static final Logger LOGGER = LoggerFactory.getLogger(ParsedRecordServiceImpl.class);

  @Override
  public Future<ParsedRecordDto> getMarcRecordByInstanceId(String instanceId, OkapiConnectionParams params) {
    Promise<ParsedRecordDto> promise = Promise.promise();
    SourceStorageClient client = new SourceStorageClient(params.getOkapiUrl(), params.getTenantId(), params.getToken());
    try {
      client.getSourceStorageSourceRecordsById(instanceId, "INSTANCE", response -> {
        if (HttpStatus.HTTP_OK.toInt() == response.statusCode()) {
          response.bodyHandler(body -> promise.handle(Try.itGet(() -> {
            return mapSourceRecordToParsedRecordDto(body);
          })));
        } else {
          String message = String.format("Error retrieving MARC record by instanceId: '%s', response code %s", instanceId, response.statusCode());
          LOGGER.error(message);
          promise.fail(message);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Error getting MARC record", e);
      promise.fail(e);
    }
    return promise.future();
  }

  private ParsedRecordDto mapSourceRecordToParsedRecordDto(Buffer body) {
    SourceRecord sourceRecord = body.toJsonObject().mapTo(SourceRecord.class);
    return new ParsedRecordDto()
      .withId(sourceRecord.getRecordId())
      .withParsedRecord(sourceRecord.getParsedRecord())
      .withRecordType(ParsedRecordDto.RecordType.fromValue(sourceRecord.getRecordType().value()))
      .withExternalIdsHolder(sourceRecord.getExternalIdsHolder())
      .withAdditionalInfo(sourceRecord.getAdditionalInfo())
      .withMetadata(sourceRecord.getMetadata());
  }
}
