package org.folio.verticle.consumers.errorhandlers.payloadbuilders;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.rest.jaxrs.model.Record.RecordType.MARC_BIB;
import static org.folio.services.util.EventHandlingUtil.populatePayloadWithHeadersData;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.MappingRuleCache;
import org.folio.services.entity.MappingRuleCacheKey;
import org.folio.services.util.RecordConversionUtil;
import org.folio.verticle.consumers.errorhandlers.RawMarcChunksErrorHandler;
import org.folio.verticle.consumers.util.MarcImportEventsHandler;

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
        DataImportEventPayload diErrorPayload = prepareDiErrorEventPayload(throwable, okapiParams, jobExecutionId, record);
        if (rulesOptional.isPresent()) {
          String recordContent = getReducedRecordContentOnlyWithTitle(rulesOptional.get(), record);
          return Future.succeededFuture(makeLightweightPayload(record, recordContent, diErrorPayload));
        }
        return Future.succeededFuture(makeLightweightPayload(record, null, diErrorPayload));
      });
  }

  private DataImportEventPayload prepareDiErrorEventPayload(Throwable throwable,
                                                            OkapiConnectionParams okapiParams,
                                                            String jobExecutionId,
                                                            Record record) {
    HashMap<String, String> context = new HashMap<>();
    context.put(RawMarcChunksErrorHandler.ERROR_KEY, throwable.getMessage());
    context.put(RecordConversionUtil.getEntityType(record).value(), Json.encode(record));

    DataImportEventPayload eventPayload = new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withJobExecutionId(jobExecutionId)
      .withOkapiUrl(okapiParams.getOkapiUrl())
      .withTenant(okapiParams.getTenantId())
      .withToken(okapiParams.getToken())
      .withContext(context);

    populatePayloadWithHeadersData(eventPayload, okapiParams);

    return eventPayload;
  }

  private String getReducedRecordContentOnlyWithTitle(JsonObject mappingRules, Record record) {
    Optional<String> titleFieldOptional = MarcImportEventsHandler.getTitleFieldTagByInstanceFieldPath(mappingRules);
    if (titleFieldOptional.isPresent()) {
      String titleFieldTag = titleFieldOptional.get();

      ParsedRecord parsedRecord = record.getParsedRecord();
      JsonObject parsedContent = new JsonObject(parsedRecord.getContent().toString());
      var fields = parsedContent.getJsonArray(FIELDS).getList();
      for (Object elem: fields) {
        Object titleField = ((Map) elem).get(titleFieldTag);
        if (titleField != null) {
          return new JsonObject()
            .put(FIELDS, new JsonArray()
              .add(new JsonObject()
                .put(titleFieldTag, titleField))).encode();
        }

      }
    }
    return null;
  }

  private DataImportEventPayload makeLightweightPayload(Record record, String newRecordContent, DataImportEventPayload payload) {
    record.setParsedRecord(StringUtils.isBlank(newRecordContent) ? null : new ParsedRecord().withContent(newRecordContent));
    record.setRawRecord(null);
    payload.setProfileSnapshot(null);
    payload.getContext().put(RecordConversionUtil.getEntityType(record).value(), Json.encode(record));
    return payload;
  }
}
