package org.folio.verticle.consumers.errorhandlers.payloadbuilders;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.MappingRuleCache;
import org.folio.services.entity.MappingRuleCacheKey;
import org.folio.services.util.RecordConversionUtil;
import org.folio.verticle.consumers.errorhandlers.RawMarcChunksErrorHandler;
import org.folio.verticle.consumers.util.MarcImportEventsHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_HOLDING;
import static org.folio.verticle.consumers.util.MarcImportEventsHandler.NO_TITLE_MESSAGE;

@Component
public class MarcHoldingsErrorPayloadBuilder implements DiErrorPayloadBuilder {
  private static final String FIELDS = "fields";

  private MappingRuleCache mappingRuleCache;

  @Autowired
  public MarcHoldingsErrorPayloadBuilder(MappingRuleCache mappingRuleCache) {
    this.mappingRuleCache = mappingRuleCache;
  }

  @Override
  public boolean isEligible(Record.RecordType recordType) {
    return MARC_HOLDING == recordType;
  }

  @Override
  public Future<DataImportEventPayload> buildEventPayload(Throwable throwable,
                                                          OkapiConnectionParams okapiParams,
                                                          String jobExecutionId,
                                                          Record record) {
        DataImportEventPayload diErrorPayload = prepareDiErrorEventPayload(throwable, okapiParams, jobExecutionId, record);
        return Future.succeededFuture(makeLightweightPayload(record, diErrorPayload));
  }

  private DataImportEventPayload prepareDiErrorEventPayload(Throwable throwable,
                                                            OkapiConnectionParams okapiParams,
                                                            String jobExecutionId, Record record) {
    HashMap<String, String> context = new HashMap<>();
    context.put(RawMarcChunksErrorHandler.ERROR_KEY, throwable.getMessage());
    context.put(RecordConversionUtil.getEntityType(record).value(), Json.encode(record));

    return new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withJobExecutionId(jobExecutionId)
      .withOkapiUrl(okapiParams.getOkapiUrl())
      .withTenant(okapiParams.getTenantId())
      .withToken(okapiParams.getToken())
      .withContext(context);

  }
  private DataImportEventPayload makeLightweightPayload(Record record, DataImportEventPayload payload) {
    record.setParsedRecord(null);
    record.setRawRecord(null);
    payload.setProfileSnapshot(null);
    payload.getContext().put(RecordConversionUtil.getEntityType(record).value(), Json.encode(record));
    return payload;
  }
}
