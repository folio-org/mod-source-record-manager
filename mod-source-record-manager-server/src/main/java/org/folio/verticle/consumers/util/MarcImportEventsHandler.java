package org.folio.verticle.consumers.util;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.ActionProfile;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.MappingRuleCache;
import org.folio.services.journal.JournalRecordMapperException;
import org.folio.services.journal.JournalService;
import org.folio.services.journal.JournalUtil;
import org.folio.services.util.ParsedRecordUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;

@Component
public class MarcImportEventsHandler implements SpecificEventHandler {

  private static final String INSTANCE_TITLE_FIELD_PATH = "title";

  private MappingRuleCache mappingRuleCache;

  @Autowired
  public MarcImportEventsHandler(MappingRuleCache mappingRuleCache) {
    this.mappingRuleCache = mappingRuleCache;
  }

  @Override
  public void handle(JournalService journalService, DataImportEventPayload eventPayload, String tenantId)
    throws JournalRecordMapperException {

    Optional<JournalParams> journalParamsOptional =
      JournalParams.JournalParamsEnum.getValue(eventPayload.getEventType()).getJournalParams(eventPayload);

    if (journalParamsOptional.isPresent()) {
      JournalParams journalParams = journalParamsOptional.get();
      JournalRecord journalRecord = JournalUtil.buildJournalRecordByEvent(eventPayload,
        journalParams.journalActionType, journalParams.journalEntityType, journalParams.journalActionStatus);

      ensureRecordTitleIfNeeded(journalRecord, eventPayload)
        .onComplete(ar -> journalService.save(JsonObject.mapFrom(journalRecord), tenantId));
    }
  }

  private Future<JournalRecord> ensureRecordTitleIfNeeded(JournalRecord journalRecord, DataImportEventPayload eventPayload) {
    String recordAsString = eventPayload.getContext().get(ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC.value());

    if (journalRecord.getEntityType().equals(MARC_BIBLIOGRAPHIC) && StringUtils.isNotBlank(recordAsString)) {
      Record record = Json.decodeValue(recordAsString, Record.class);
      if (record.getParsedRecord() != null) {
        return mappingRuleCache.get(eventPayload.getTenant())
          .compose(ruleOptional -> ruleOptional
            .map(mappingRules -> retrieveTitle(record, mappingRules))
            .map(title -> Future.succeededFuture(journalRecord.withTitle(title)))
            .orElse(Future.succeededFuture(journalRecord)));
      }
    }
    return Future.succeededFuture(journalRecord);
  }

  private String retrieveTitle(Record record, JsonObject mappingRules) {
    Optional<String> titleFieldOptional = getTitleFieldTagByInstanceFieldPath(mappingRules);

    if (titleFieldOptional.isPresent()) {
      String titleFieldTag = titleFieldOptional.get();
      List<String>  subfieldCodes = mappingRules.getJsonArray(titleFieldTag).stream()
        .map(JsonObject.class::cast)
        .filter(fieldMappingRule -> fieldMappingRule.getString("target").equals(INSTANCE_TITLE_FIELD_PATH))
        .flatMap(fieldMappingRule -> fieldMappingRule.getJsonArray("subfield").stream())
        .map(Object::toString)
        .collect(Collectors.toList());

      return subfieldCodes.isEmpty() ? null : ParsedRecordUtil.retrieveDataByField(record.getParsedRecord(), titleFieldTag, subfieldCodes);
    }
    return null;
  }

  private Optional<String> getTitleFieldTagByInstanceFieldPath(JsonObject mappingRules) {
    return mappingRules.getMap().keySet().stream()
      .filter(fieldTag -> mappingRules.getJsonArray(fieldTag).stream()
        .map(o -> (JsonObject) o)
        .anyMatch(fieldMappingRule -> INSTANCE_TITLE_FIELD_PATH.equals(fieldMappingRule.getString("target"))))
      .findFirst();
  }
}
