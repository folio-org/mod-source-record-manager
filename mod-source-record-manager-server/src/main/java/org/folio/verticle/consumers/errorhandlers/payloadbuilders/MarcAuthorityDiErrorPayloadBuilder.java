package org.folio.verticle.consumers.errorhandlers.payloadbuilders;

import io.vertx.core.Future;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.Record;
import org.folio.verticle.consumers.util.DiErrorBuilderUtil;
import org.springframework.stereotype.Component;

import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_AUTHORITY;
import static org.folio.verticle.consumers.util.MarcImportEventsHandler.NO_TITLE_MESSAGE;

@Component
public class MarcAuthorityDiErrorPayloadBuilder implements DiErrorPayloadBuilder {
  @Override
  public boolean isEligible(Record.RecordType recordType) {
    return MARC_AUTHORITY == recordType;
  }

  @Override
  public Future<DataImportEventPayload> buildEventPayload(Throwable throwable,
                                                          OkapiConnectionParams okapiParams,
                                                          String jobExecutionId,
                                                          Record currentRecord) {
    DataImportEventPayload diErrorPayload = DiErrorBuilderUtil.prepareDiErrorEventPayload(throwable, okapiParams, jobExecutionId, currentRecord);
    return Future.succeededFuture(DiErrorBuilderUtil.makeLightweightPayload(currentRecord, NO_TITLE_MESSAGE, diErrorPayload));
  }
}
