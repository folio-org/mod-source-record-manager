package org.folio.verticle.consumers.errorhandlers.payloadbuilders;

import io.vertx.core.Future;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.MappingRuleCache;
import org.folio.services.entity.MappingRuleCacheKey;
import org.folio.verticle.consumers.util.DiErrorBuilderUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;

@Component
public class MarcBibDiErrorPayloadBuilder implements DiErrorPayloadBuilder {
  private final MappingRuleCache mappingRuleCache;

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
