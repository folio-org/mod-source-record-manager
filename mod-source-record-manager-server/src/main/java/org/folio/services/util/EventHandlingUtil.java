package org.folio.services.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.processing.events.utils.ZIPArchiver;
import org.folio.rest.jaxrs.model.Event;
import org.folio.rest.jaxrs.model.EventMetadata;
import org.folio.services.EventDrivenChunkProcessingServiceImpl;
import org.folio.util.pubsub.PubSubClientUtils;

import java.util.UUID;

@Deprecated
public final class EventHandlingUtil {

  private EventHandlingUtil() {
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(EventHandlingUtil.class);

  /**
   * Prepares and sends event with zipped payload to the mod-pubsub
   *
   * @param eventPayload eventPayload in String representation
   * @param eventType    eventType
   * @param params       connection parameters
   * @return completed future with true if event was sent successfully
   */
  @Deprecated
  public static Future<Boolean> sendEventWithPayload(String eventPayload, String eventType, OkapiConnectionParams params, EventDrivenChunkProcessingServiceImpl service) {
    Promise<Boolean> promise = Promise.promise();
    try {
      Event event = new Event()
        .withId(UUID.randomUUID().toString())
        .withEventType(eventType)
        .withEventPayload(ZIPArchiver.zip(eventPayload))
        .withEventMetadata(new EventMetadata()
          .withTenantId(params.getTenantId())
          .withEventTTL(1)
          .withPublishedBy(PubSubClientUtils.constructModuleName()));

      org.folio.rest.util.OkapiConnectionParams connectionParams = new org.folio.rest.util.OkapiConnectionParams();
      connectionParams.setOkapiUrl(params.getOkapiUrl());
      connectionParams.setToken(params.getToken());
      connectionParams.setTenantId(params.getTenantId());
      connectionParams.setVertx(params.getVertx());

      service.sendEvent(event, params.getTenantId()).onComplete(v -> promise.complete(true));
    } catch (Exception e) {
      LOGGER.error("Failed to send {} event to mod-pubsub", e, eventType);
      promise.fail(e);
    }
    return promise.future();
  }
}
