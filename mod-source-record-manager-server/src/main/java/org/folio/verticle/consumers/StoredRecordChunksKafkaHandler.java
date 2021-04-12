package org.folio.verticle.consumers;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import io.vertx.core.json.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.DataImportEventTypes;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.RecordType;
import org.folio.rest.jaxrs.model.RecordsBatchResponse;
import org.folio.services.RecordsPublishingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_EDIFACT_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_SRS_MARC_BIB_RECORD_CREATED;
import static org.folio.rest.jaxrs.model.Record.RecordType.EDIFACT;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC;

@Component
@Qualifier("StoredRecordChunksKafkaHandler")
public class StoredRecordChunksKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LogManager.getLogger();

  private static final Map<RecordType, DataImportEventTypes> RECORD_TYPE_TO_EVENT_TYPE = Map.of(
    MARC, DI_SRS_MARC_BIB_RECORD_CREATED,
    EDIFACT, DI_EDIFACT_RECORD_CREATED
  );

  private RecordsPublishingService recordsPublishingService;
  private KafkaInternalCache kafkaInternalCache;
  private Vertx vertx;

  public StoredRecordChunksKafkaHandler(@Autowired @Qualifier("recordsPublishingService") RecordsPublishingService recordsPublishingService,
                                        @Autowired KafkaInternalCache kafkaInternalCache,
                                        @Autowired Vertx vertx) {
    this.recordsPublishingService = recordsPublishingService;
    this.kafkaInternalCache = kafkaInternalCache;
    this.vertx = vertx;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
    String correlationId = okapiConnectionParams.getHeaders().get("correlationId");
    String chunkNumber = okapiConnectionParams.getHeaders().get("chunkNumber");

    Event event = Json.decodeValue(record.value(), Event.class);

    if (!kafkaInternalCache.containsByKey(event.getId())) {
      try {
        kafkaInternalCache.putToCache(event.getId());
        String unzipped = ZIPArchiver.unzip(event.getEventPayload());
        RecordsBatchResponse recordsBatchResponse = new JsonObject(unzipped).mapTo(RecordsBatchResponse.class);
        List<Record> storedRecords = recordsBatchResponse.getRecords();

        // we only know record type by inspecting the records, assuming records are homogeneous type and defaulting to previous static value
        DataImportEventTypes eventType = !storedRecords.isEmpty() && RECORD_TYPE_TO_EVENT_TYPE.containsKey(storedRecords.get(0).getRecordType())
          ? RECORD_TYPE_TO_EVENT_TYPE.get(storedRecords.get(0).getRecordType())
          : DI_SRS_MARC_BIB_RECORD_CREATED;

        LOGGER.debug("RecordsBatchResponse has been received, starting processing correlationId: {} chunkNumber: {}", correlationId, chunkNumber);
        return recordsPublishingService.sendEventsWithRecords(storedRecords, okapiConnectionParams.getHeaders().get("jobExecutionId"),
          okapiConnectionParams, eventType.value())
          .compose(b -> {
            LOGGER.debug("RecordsBatchResponse processing has been completed correlationId: {} chunkNumber: {}", correlationId, chunkNumber);
            return Future.succeededFuture(correlationId);
          }, th -> {
            LOGGER.error("RecordsBatchResponse processing has failed with errors correlationId: {} chunkNumber: {}", correlationId, chunkNumber, th);
            return Future.failedFuture(th);
          });
      } catch (IOException e) {
        LOGGER.error("Can't process kafka record: ", e);
        return Future.failedFuture(e);
      }
    }
    return Future.succeededFuture(record.key());
  }

}
