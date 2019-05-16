package org.folio.services.afterprocessing;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.Record;
import org.springframework.stereotype.Component;

/**
 * Util to work with additional fields
 */
@Component
public class AdditionalFieldsUtil {
  private static final Logger LOGGER = LoggerFactory.getLogger(AdditionalFieldsUtil.class);

  /**
   * Adds inventory instance id into MARC record
   *
   * @param record     record
   * @param instanceId UUID of Instance entity
   * @return true if instanceId is added to parsed record, else returns false
   */
  public boolean addInstanceIdToMarcRecord(Record record, String instanceId) {
    if (record.getParsedRecord() != null) {
      Object content = record.getParsedRecord().getContent();
      if (content != null) {
        String stringContent = content.toString();
        if (StringUtils.isNotEmpty(stringContent)) {
          JsonObject jsonContent = null;
          try {
            jsonContent = new JsonObject(stringContent);
          } catch (Exception exception) {
            LOGGER.error("Can not convert parsed record content to JsonObject. Cause:{}", exception.getCause());
            return false;
          }
          if (jsonContent.containsKey("fields")) {
            JsonArray fields = jsonContent.getJsonArray("fields");
            for (int i = fields.size(); i-- > 0; ) {
              JsonObject targetField = fields.getJsonObject(i);
              if (targetField.containsKey(AdditionalFieldsConfig.TAG_999)) {
                JsonObject instanceIdSubField = new JsonObject().put("i", instanceId);
                targetField.getJsonObject(AdditionalFieldsConfig.TAG_999).getJsonArray("subfields").add(instanceIdSubField);
                record.getParsedRecord().setContent(jsonContent.toString());
                return true;
              }
            }
          }
        }
      }
    }
    return false;
  }
}
