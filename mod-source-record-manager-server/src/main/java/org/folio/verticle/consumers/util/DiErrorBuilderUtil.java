package org.folio.verticle.consumers.util;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.Record;
import org.folio.services.util.RecordConversionUtil;
import org.folio.verticle.consumers.errorhandlers.RawMarcChunksErrorHandler;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.folio.rest.jaxrs.model.DataImportEventTypes.DI_ERROR;
import static org.folio.verticle.consumers.util.MarcImportEventsHandler.NO_TITLE_MESSAGE;

/**
 * Util class for processing data-import errors for different types
 * @author Volodymyr_Rohach
 */
public final class DiErrorBuilderUtil {

  private DiErrorBuilderUtil() {

  }

  private static final String FIELDS = "fields";

  public static DataImportEventPayload prepareDiErrorEventPayload(Throwable throwable,
                                                                  OkapiConnectionParams okapiParams,
                                                                  String jobExecutionId, Record currentRecord) {
    HashMap<String, String> context = new HashMap<>();
    context.put(RawMarcChunksErrorHandler.ERROR_KEY, throwable.getMessage());
    context.put(RecordConversionUtil.getEntityType(currentRecord).value(), Json.encode(currentRecord));

    return new DataImportEventPayload()
      .withEventType(DI_ERROR.value())
      .withJobExecutionId(jobExecutionId)
      .withOkapiUrl(okapiParams.getOkapiUrl())
      .withTenant(okapiParams.getTenantId())
      .withToken(okapiParams.getToken())
      .withContext(context);
  }

  public static DataImportEventPayload makeLightweightPayload(Record currentRecord, String newRecordContent, DataImportEventPayload payload) {
    currentRecord.setParsedRecord(StringUtils.isBlank(newRecordContent) ? null : new ParsedRecord().withContent(newRecordContent));
    currentRecord.setParsedRecord(null);
    currentRecord.setRawRecord(null);
    payload.setProfileSnapshot(null);
    payload.getContext().put(RecordConversionUtil.getEntityType(currentRecord).value(), Json.encode(currentRecord));
    return payload;
  }

  public static String getReducedRecordContentOnlyWithTitle(JsonObject mappingRules, Record currentRecord) {
    Optional<String> titleFieldOptional = MarcImportEventsHandler.getTitleFieldTagByInstanceFieldPath(mappingRules);
    if (titleFieldOptional.isPresent()) {
      String titleFieldTag = titleFieldOptional.get();

      ParsedRecord parsedRecord = currentRecord.getParsedRecord();
      if (parsedRecord == null) {
        return new JsonObject()
          .put(FIELDS, new JsonArray()
            .add(new JsonObject()
              .put(titleFieldTag, NO_TITLE_MESSAGE))).encode();
      }
      JsonObject parsedContent = new JsonObject(parsedRecord.getContent().toString());
      var fields = parsedContent.getJsonArray(FIELDS).getList();
      for (Object elem : fields) {
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
}
