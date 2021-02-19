package org.folio.verticle.consumers;

import java.io.IOException;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
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

@Component
@Qualifier("StoredMarcChunksKafkaHandler")
public class StoredMarcChunksKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LogManager.getLogger();

  private RecordsPublishingService recordsPublishingService;
  private Vertx vertx;

  public StoredMarcChunksKafkaHandler(@Autowired @Qualifier("recordsPublishingService") RecordsPublishingService recordsPublishingService,
                                      @Autowired Vertx vertx) {
    this.recordsPublishingService = recordsPublishingService;
    this.vertx = vertx;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
    String correlationId = okapiConnectionParams.getHeaders().get("correlationId");
    String chunkNumber = okapiConnectionParams.getHeaders().get("chunkNumber");

    Event event = new JsonObject(record.value()).mapTo(Event.class);

    try {
      String unzipped = ZIPArchiver.unzip(event.getEventPayload());
      RecordsBatchResponse recordsBatchResponse = new JsonObject(unzipped).mapTo(RecordsBatchResponse.class);
      List<Record> storedRecords = recordsBatchResponse.getRecords();
      // default to previous static value
      String eventType = DI_SRS_MARC_BIB_RECORD_CREATED.value();
      // we only know record type by inspecting the records, assuming records are homogeneous type
      if (!storedRecords.isEmpty()) {
        RecordType recordType = storedRecords.get(0).getRecordType();
        eventType = RecordTypeToCreatedEventType.valueOf(recordType.value()).getCreatedEventType();
      }

      LOGGER.debug("RecordsBatchResponse has been received, starting processing correlationId: {} chunkNumber: {}", correlationId, chunkNumber);
      return recordsPublishingService.sendEventsWithRecords(storedRecords, okapiConnectionParams.getHeaders().get("jobExecutionId"),
        okapiConnectionParams, eventType)
        .compose(b -> {
          LOGGER.debug("RecordsBatchResponse processing has been completed correlationId: {} chunkNumber: {}",correlationId, chunkNumber);
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

  /**
   * Enum mapping from {@link RecordType} to {@link DataImportEventTypes} for created events.
   */
  private enum RecordTypeToCreatedEventType {
    MARC(DI_SRS_MARC_BIB_RECORD_CREATED),
    EDIFACT(DI_EDIFACT_RECORD_CREATED);

    private String createdEventType;

    private RecordTypeToCreatedEventType(DataImportEventTypes createdEventType) {
      this.createdEventType = createdEventType.value();
    }

    public String getCreatedEventType() {
      return this.createdEventType;
    }

  }

}
