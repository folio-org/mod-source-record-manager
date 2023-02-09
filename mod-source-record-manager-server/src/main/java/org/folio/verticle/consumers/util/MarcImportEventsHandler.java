package org.folio.verticle.consumers.util;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.JournalRecordService;
import org.folio.services.MappingRuleCache;
import org.folio.services.entity.MappingRuleCacheKey;
import org.folio.services.journal.JournalRecordMapperException;
import org.folio.services.journal.JournalService;
import org.folio.services.journal.JournalUtil;
import org.folio.services.util.ParsedRecordUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.ITEM;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_AUTHORITY;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.JournalRecord.EntityType.PO_LINE;

@Component
public class MarcImportEventsHandler implements SpecificEventHandler {

  public static final String INSTANCE_TITLE_FIELD_PATH = "title";

  public static final String NO_TITLE_MESSAGE = "No content";

  private static final Map<JournalRecord.EntityType, BiFunction<ParsedRecord, JsonObject, String>> titleExtractorMap =
    Map.of(
      MARC_BIBLIOGRAPHIC, marcBibTitleExtractor(),
      MARC_AUTHORITY, marcAuthorityTitleExtractor()
    );
  public static final String PO_LINE_KEY = "PO_LINE";
  public static final String PO_LINE_TITLE = "titleOrPackage";

  private final MappingRuleCache mappingRuleCache;

  private JournalRecordService journalRecordService;

  @Autowired
  public MarcImportEventsHandler(MappingRuleCache mappingRuleCache, JournalRecordService journalRecordService) {
    this.mappingRuleCache = mappingRuleCache;
    this.journalRecordService = journalRecordService;
  }

  private static BiFunction<ParsedRecord, JsonObject, String> marcBibTitleExtractor() {
    return (parsedRecord, mappingRules) -> {
      Optional<String> titleFieldOptional = getTitleFieldTagByInstanceFieldPath(mappingRules);

      if (titleFieldOptional.isPresent()) {
        String titleFieldTag = titleFieldOptional.get();
        List<String> subfieldCodes = mappingRules.getJsonArray(titleFieldTag).stream()
          .map(JsonObject.class::cast)
          .filter(fieldMappingRule -> fieldMappingRule.getString("target").equals(INSTANCE_TITLE_FIELD_PATH))
          .flatMap(fieldMappingRule -> fieldMappingRule.getJsonArray("subfield").stream())
          .map(Object::toString)
          .collect(Collectors.toList());

        return subfieldCodes.isEmpty()
          ? null
          : ParsedRecordUtil.retrieveDataByField(parsedRecord, titleFieldTag, subfieldCodes);
      }
      return null;
    };
  }

  private static BiFunction<ParsedRecord, JsonObject, String> marcAuthorityTitleExtractor() {
    return (parsedRecord, mappingRules) -> IntStream.range(100, 199)
      .mapToObj(String::valueOf)
      .map(tagCode -> ParsedRecordUtil.retrieveDataByField(parsedRecord, tagCode))
      .filter(StringUtils::isNotBlank)
      .findFirst()
      .orElse(null);
  }

  public static Optional<String> getTitleFieldTagByInstanceFieldPath(JsonObject mappingRules) {
    return mappingRules.getMap().keySet().stream()
      .filter(fieldTag -> mappingRules.getJsonArray(fieldTag).stream()
        .map(JsonObject.class::cast)
        .anyMatch(fieldMappingRule -> INSTANCE_TITLE_FIELD_PATH.equals(fieldMappingRule.getString("target"))))
      .findFirst();
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

      if (Objects.equals(journalRecord.getEntityType(), PO_LINE)) {
        processJournalRecordForOrder(journalService, tenantId, journalRecord);
      } else {
        populateRecordTitleIfNeeded(journalRecord, eventPayload)
          .onComplete(ar -> journalService.save(JsonObject.mapFrom(journalRecord), tenantId));
      }
    }
  }

  private void processJournalRecordForOrder(JournalService journalService, String tenantId, JournalRecord journalRecord) {
    if (journalRecord.getOrderId() != null && journalRecord.getError() != null) {
      journalRecordService.updateErrorJournalRecordsByOrderIdAndJobExecution(journalRecord.getJobExecutionId(), journalRecord.getOrderId(), journalRecord.getError(), tenantId)
        .onComplete(e -> journalService.save(JsonObject.mapFrom(journalRecord), tenantId));
    } else {
      journalService.save(JsonObject.mapFrom(journalRecord), tenantId);
    }
  }
  private Future<JournalRecord> populateRecordTitleIfNeeded(JournalRecord journalRecord,
                                                            DataImportEventPayload eventPayload) {
    var entityType = (journalRecord.getEntityType() == HOLDINGS || journalRecord.getEntityType() == ITEM ?
      MARC_BIBLIOGRAPHIC : journalRecord.getEntityType());

    if (entityType == MARC_BIBLIOGRAPHIC || entityType == MARC_AUTHORITY) {
      journalRecord.setTitle(NO_TITLE_MESSAGE);
      String recordAsString = eventPayload.getContext().get(entityType.value());
      if (StringUtils.isNotBlank(recordAsString)) {
        var parsedRecord = Json.decodeValue(recordAsString, Record.class).getParsedRecord();
        return mappingRuleCache.get(new MappingRuleCacheKey(eventPayload.getTenant(), entityType))
          .compose(ruleOptional -> ruleOptional
            .map(mappingRules -> {
              var titleExtractor = titleExtractorMap.get(entityType);
              if (titleExtractor == null || parsedRecord == null) {
                return null;
              }

              return titleExtractor.apply(parsedRecord, mappingRules);
            })
            .map(title -> Future.succeededFuture(journalRecord.withTitle(title)))
            .orElseGet(() -> Future.succeededFuture(journalRecord)));
      }
    }

    return Future.succeededFuture(journalRecord);
  }
}
