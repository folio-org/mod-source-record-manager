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
import org.folio.verticle.consumers.errorhandlers.RawMarcChunksErrorHandler;
import org.folio.services.util.RecordConversionUtil;
import org.folio.verticle.consumers.util.DiErrorBuilderUtil;
import org.folio.verticle.consumers.util.MarcImportEventsHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.verticle.consumers.util.MarcImportEventsHandler.NO_TITLE_MESSAGE;

@Component
public class MarcBibDiErrorPayloadBuilder implements DiErrorPayloadBuilder {
  private static final String FIELDS = "fields";
  private MappingRuleCache mappingRuleCache;

  @Autowired
  public MarcBibDiErrorPayloadBuilder(MappingRuleCache mappingRuleCache) {
    this.mappingRuleCache = mappingRuleCache;
  }

  @Override
  public boolean isEligible(Record.RecordType recordType) {
    return MARC_BIB == recordType;
  }

  @Override
  public Future<DataImportEventPayload> buildEventPayload(Throwable throwable,
                                                          OkapiConnectionParams okapiParams,
                                                          String jobExecutionId,
                                                          Record record) {

    return mappingRuleCache.get(new MappingRuleCacheKey(okapiParams.getTenantId(), record.getRecordType()))
      .compose(rulesOptional -> {
        DataImportEventPayload diErrorPayload = DiErrorBuilderUtil.prepareDiErrorEventPayload(throwable, okapiParams, jobExecutionId, record);
        if (rulesOptional.isPresent()) {
          String recordContent = DiErrorBuilderUtil.getReducedRecordContentOnlyWithTitle(rulesOptional.get(), record);
          return Future.succeededFuture(DiErrorBuilderUtil.makeLightweightPayload(record, recordContent, diErrorPayload));
        }
        return Future.succeededFuture(DiErrorBuilderUtil.makeLightweightPayload(record, null, diErrorPayload));
      });
  }
}
