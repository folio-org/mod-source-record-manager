package org.folio.services.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.HashMap;
import java.util.UUID;

import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.DataImportEventPayload;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.util.pubsub.PubSubClientUtils;

import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;

public final class EventHandlingUtil {

  private EventHandlingUtil() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(EventHandlingUtil.class);

  /**
   * Prepares event with record, profileSnapshotWrapper and sends prepared event with zipped payload to the mod-pubsub
   *
   * @param record                 record to send
   * @param profileSnapshotWrapper profileSnapshotWrapper to send
   * @param mappingRules           rules for default instance mapping
   * @param mappingParameters      mapping parameters
   * @param params                 connection parameters
   * @return completed future with record if record was sent successfully
   */
  public static Future<Record> sendEventWithRecord(Record record, String eventType, ProfileSnapshotWrapper profileSnapshotWrapper,
                                                   JsonObject mappingRules, MappingParameters mappingParameters, OkapiConnectionParams params) {
    Promise<Record> promise = Promise.promise();
    try {
      HashMap<String, String> dataImportEventPayloadContext = new HashMap<>();
      dataImportEventPayloadContext.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
      dataImportEventPayloadContext.put("MAPPING_RULES", mappingRules.encode());
      dataImportEventPayloadContext.put("MAPPING_PARAMS", Json.encode(mappingParameters));

      DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
        .withEventType(eventType)
        .withProfileSnapshot(profileSnapshotWrapper)
        .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
        .withJobExecutionId(record.getSnapshotId())
        .withContext(dataImportEventPayloadContext)
        .withOkapiUrl(params.getOkapiUrl())
        .withTenant(params.getTenantId())
        .withToken(params.getToken());

      Event createdRecordEvent = new Event()
        .withId(UUID.randomUUID().toString())
        .withEventType(eventType)
        .withEventPayload(ZIPArchiver.zip(Json.encode(dataImportEventPayload)))
        .withEventMetadata(new EventMetadata()
          .withTenantId(params.getTenantId())
          .withEventTTL(1)
          .withPublishedBy(PubSubClientUtils.constructModuleName()));

      org.folio.rest.util.OkapiConnectionParams connectionParams = new org.folio.rest.util.OkapiConnectionParams();
      connectionParams.setOkapiUrl(params.getOkapiUrl());
      connectionParams.setToken(params.getToken());
      connectionParams.setTenantId(params.getTenantId());
      connectionParams.setVertx(params.getVertx());

      PubSubClientUtils.sendEventMessage(createdRecordEvent, connectionParams)
        .whenComplete((ar, throwable) -> {
          if (throwable == null) {
            promise.complete(record);
          } else {
            LOGGER.error("Error during event sending: {}", throwable, createdRecordEvent);
            promise.fail(throwable);
          }
        });
    } catch (Exception e) {
      LOGGER.error("Error during event building or sending: {}", e);
      promise.fail(e);
    }
    return promise.future();
  }
}
