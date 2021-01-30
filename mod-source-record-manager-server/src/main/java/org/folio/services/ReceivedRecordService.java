package org.folio.services;

import io.vertx.core.Future;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.Record;

import java.util.List;

/**
 * Service for processing parsed record
 */
public interface ReceivedRecordService {

   Future<Boolean> sendEventsWithRecords(List<Record> records, String jobExecutionId, OkapiConnectionParams params, String eventType);

}
