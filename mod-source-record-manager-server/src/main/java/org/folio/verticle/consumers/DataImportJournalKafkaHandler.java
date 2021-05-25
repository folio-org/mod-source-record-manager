package org.folio.verticle.consumers;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.kafka.AsyncRecordHandler;
import org.folio.kafka.KafkaHeaderUtils;
import org.folio.kafka.cache.KafkaInternalCache;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.services.journal.JournalService;
import org.folio.verticle.consumers.util.EventTypeHandlerSelector;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;

import static java.lang.String.format;
import static org.folio.services.RecordsPublishingServiceImpl.CORRELATION_ID_HEADER;

@Component
@Qualifier("DataImportJournalKafkaHandler")
public class DataImportJournalKafkaHandler implements AsyncRecordHandler<String, String> {
  private static final Logger LOGGER = LogManager.getLogger();
  private static final String EVENT_ID_PREFIX = DataImportJournalKafkaHandler.class.getSimpleName();

  private Vertx vertx;
  private JournalService journalService;
  private KafkaInternalCache kafkaInternalCache;
  private EventTypeHandlerSelector eventTypeHandlerSelector = new EventTypeHandlerSelector();

  public DataImportJournalKafkaHandler(@Autowired Vertx vertx,
                                       @Autowired KafkaInternalCache kafkaInternalCache,
                                       @Autowired @Qualifier("journalServiceProxy") JournalService journalService) {
    this.vertx = vertx;
    this.journalService = journalService;
    this.kafkaInternalCache = kafkaInternalCache;
  }

  @Override
  public Future<String> handle(KafkaConsumerRecord<String, String> record) {
    Promise<String> result = Promise.promise();
    List<KafkaHeader> kafkaHeaders = record.headers();
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(KafkaHeaderUtils.kafkaHeadersToMap(kafkaHeaders), vertx);
    String correlationId = okapiConnectionParams.getHeaders().get(CORRELATION_ID_HEADER);
    Event event = new JsonObject(record.value()).mapTo(Event.class);
    LOGGER.debug("Event was received with correlationId: {} event type: {}", correlationId, event.getEventType());
    String handlerBasedEventId = format("%s-%s", EVENT_ID_PREFIX, event.getId());

    try {
      if (!kafkaInternalCache.containsByKey(handlerBasedEventId)) {
        kafkaInternalCache.putToCache(handlerBasedEventId);
        DataImportEventPayload eventPayload = new ObjectMapper().readValue(ZIPArchiver.unzip(event.getEventPayload()), DataImportEventPayload.class);
        LOGGER.debug("LOGGING EVENT: {}", eventPayload);
        eventTypeHandlerSelector.getHandler(eventPayload).handle(journalService, eventPayload, okapiConnectionParams.getTenantId());
      }
      result.complete(record.key());
    } catch (Exception e) {
      LOGGER.error("Error during processing journal event", e);
      result.fail(e);
    }
    return result.future();
  }

}
