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

import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_AUTHORITY;

@Component
public class MarcAuthorityDiErrorPayloadBuilder implements DiErrorPayloadBuilder {
  private MappingRuleCache mappingRuleCache;

  @Autowired
  public MarcAuthorityDiErrorPayloadBuilder(MappingRuleCache mappingRuleCache) {
    this.mappingRuleCache = mappingRuleCache;
  }

  @Override
  public boolean isEligible(Record.RecordType recordType) {
    return MARC_AUTHORITY == recordType;
  }

  @Override
  public Future<DataImportEventPayload> buildEventPayload(Throwable throwable,
                                                          OkapiConnectionParams okapiParams,
                                                          String jobExecutionId,
                                                          Record currentRecord) {

    return mappingRuleCache.get(new MappingRuleCacheKey(okapiParams.getTenantId(), currentRecord.getRecordType()))
      .compose(rulesOptional -> {
        DataImportEventPayload diErrorPayload = DiErrorBuilderUtil.prepareDiErrorEventPayload(throwable, okapiParams, jobExecutionId, currentRecord);
        if (rulesOptional.isPresent()) {
          String recordContent = DiErrorBuilderUtil.getReducedRecordContentOnlyWithTitle(rulesOptional.get(), currentRecord);
          return Future.succeededFuture(DiErrorBuilderUtil.makeLightweightPayload(currentRecord, recordContent, diErrorPayload));
        }
        return Future.succeededFuture(DiErrorBuilderUtil.makeLightweightPayload(currentRecord, null, diErrorPayload));
      });
  }
}
