package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_BIB;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Resources;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.services.MappingRuleService;
import org.folio.services.migration.CustomMigration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class MarcBibMapping338FormatCustomMigration implements CustomMigration {

  private static final Logger LOGGER = LogManager.getLogger();

  private static final String UNMODIFIED_RULE = "migration/marc_bib/338/unmodified.json";
  private static final String UPDATED_RULE = "migration/marc_bib/338/updated.json";
  private static final String RULE_TAG = "338";

  @Autowired
  private MappingRuleService mappingRuleService;

  private final ObjectMapper mapper = new ObjectMapper();

  @Override
  public Future<Void> migrate(String tenantId) {
    return mappingRuleService.get(MARC_BIB, tenantId)
      .compose(rules -> {
        if (rules.isPresent()) {
          var newRules = updateRules(rules.get());
          return mappingRuleService.update(newRules.encode(), MARC_BIB, tenantId);
        } else {
          return Future.succeededFuture();
        }
      }).mapEmpty();
  }

  @Override
  public String getFeatureVersion() {
    return "4.0.0";
  }

  @Override
  public String getDescription() {
    return "MARC Bib mapping rules: update rule for formatId in 338 field";
  }

  private JsonObject updateRules(JsonObject rules) {
    try {
      var ruleArray = rules.getJsonArray(RULE_TAG);
      if (ruleArray == null) {
        return rules;
      }
      JsonNode currentRule = mapper.readTree(ruleArray.encode());
      JsonNode unmodifiedRule = mapper.readTree(Resources.toByteArray(Resources.getResource(UNMODIFIED_RULE)));
      if (currentRule.equals(unmodifiedRule)) {
        JsonArray updatedRule = new JsonArray(Resources.toString(Resources.getResource(UPDATED_RULE), StandardCharsets.UTF_8));
        rules.put(RULE_TAG, updatedRule);
        LOGGER.info("Updated 338 rule in marc-bib mapping rules");
      } else {
        LOGGER.warn("338 rule is customized and was not updated");
      }
    } catch (Exception e) {
      LOGGER.warn("Cannot update 338 rule in marc-bib mapping rules due to {}", e.getMessage());
    }

    return rules;
  }
}
