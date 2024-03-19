package org.folio.services;

import io.vertx.core.Future;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.Record;

import java.util.List;
import java.util.Map;

/**
 * Service for publishing events with records
 */
public interface RecordsPublishingService {

  /**
   * Publishes an event with each of the passed records to the specified topic
   *
   * @param records        records to publish
   * @param jobExecutionId job execution id
   * @param params         okapi connection params
   * @param eventType      event type
   * @return true if successful
   */
  Future<Boolean> sendEventsWithRecords(List<Record> records, String jobExecutionId, OkapiConnectionParams params, String eventType, Map<String, String> context);

}
