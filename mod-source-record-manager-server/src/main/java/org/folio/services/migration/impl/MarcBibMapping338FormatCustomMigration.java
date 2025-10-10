package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_BIB;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.services.MappingRuleService;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Component
public class MarcBibMapping338FormatCustomMigration extends BaseMappingRulesMigration {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final int ORDER = 14;
  private static final UUID MIGRATION_ID = UUID.fromString("a65b7666-bba8-4c5b-bad3-0410015250cd");
  private static final String DESCRIPTION = "MARC Bib mapping rules: update rule for formatId in 338 field";
  private static final String UNMODIFIED_RULE = "migration/marc_bib/338/unmodified.json";
  private static final String UPDATED_RULE = "migration/marc_bib/338/updated.json";
  private static final String RULE_TAG = "338";

  protected MarcBibMapping338FormatCustomMigration(MappingRuleService mappingRuleService) {
    super(MARC_BIB, ORDER, DESCRIPTION, MIGRATION_ID, mappingRuleService);
  }

  @Override
  protected String updateRules(JsonObject rules) {
    try {
      var ruleArray = rules.getJsonArray(RULE_TAG);
      if (ruleArray == null) {
        return rules.encode();
      }
      JsonNode currentRule = MAPPER.readTree(ruleArray.encode());
      JsonNode unmodifiedRule = MAPPER.readTree(Resources.toByteArray(Resources.getResource(UNMODIFIED_RULE)));
      if (currentRule.equals(unmodifiedRule)) {
        JsonArray updatedRule = new JsonArray(Resources.toString(Resources.getResource(UPDATED_RULE), StandardCharsets.UTF_8));
        rules.put(RULE_TAG, updatedRule);
        LOGGER.info("Updated 338 rule in marc-bib mapping rules");
      } else {
        LOGGER.warn("338 rule is customized and was not updated");
      }
    } catch (Exception e) {
      LOGGER.warn("Cannot update 338 rule in marc-bib mapping rules", e);
    }

    return rules.encode();
  }
}
