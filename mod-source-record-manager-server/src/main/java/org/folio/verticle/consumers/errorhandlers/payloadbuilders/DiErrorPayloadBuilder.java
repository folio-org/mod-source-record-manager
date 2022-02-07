package org.folio.verticle.consumers.errorhandlers.payloadbuilders;

import io.vertx.core.Future;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Record.RecordType;

public interface DiErrorPayloadBuilder {

  /**
   * Checks if particular record type is eligible for payload builder.
   *
   * @param recordType the record type
   * @return true if eligible or false otherwise
   */
  boolean isEligible(RecordType recordType);

  /**
   * Builds event payload to be used for sending DI_ERROR events. Payloads should be parsed and log entries created
   * to show particular error message for each log entry separately.
   * Imports should finish with Completed with errors status instead of Fail or stuck.
   *
   * @param throwable throwable that occurs
   * @param okapiParams the okapi params
   * @param jobExecutionId the job execution id
   * @param record the incoming record
   * @return event payload with DI_ERROR event type and error message
   */
  Future<DataImportEventPayload> buildEventPayload(Throwable throwable,
                                                   OkapiConnectionParams okapiParams,
                                                   String jobExecutionId,
                                                   Record record);
}
