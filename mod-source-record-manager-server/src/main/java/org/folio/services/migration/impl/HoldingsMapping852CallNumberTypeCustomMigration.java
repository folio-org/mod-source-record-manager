package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_HOLDING;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.UUID;
import org.folio.Record;
import org.folio.services.MappingRuleService;
import org.folio.services.migration.CustomMigration;
import org.springframework.stereotype.Component;

@Component
public class HoldingsMapping852CallNumberTypeCustomMigration implements CustomMigration {

  private static final int ORDER = 1;
  private static final UUID MIGRATION_ID = UUID.fromString("7a7a4270-7a9a-420e-a27e-1763ed03a57f");

  private final MappingRuleService mappingRuleService;

  public HoldingsMapping852CallNumberTypeCustomMigration(MappingRuleService mappingRuleService) {
    this.mappingRuleService = mappingRuleService;
  }

  @Override
  public Future<Void> migrate(String tenantId) {
    return mappingRuleService.get(MARC_HOLDING, tenantId)
      .compose(rules -> {
        if (rules.isPresent()) {
          var newRules = updateRules(rules.get());
          return mappingRuleService.update(newRules.encode(), MARC_HOLDING, tenantId);
        } else {
          return Future.succeededFuture();
        }
      }).mapEmpty();
  }

  @Override
  public UUID getMigrationId() {
    return MIGRATION_ID;
  }

  @Override
  public int getOrder() {
    return ORDER;
  }

  @Override
  public Record.RecordType getRecordType() {
    return MARC_HOLDING;
  }

  @Override
  public String getDescription() {
    return "Holdings mapping rules: update rule for callNumberType";
  }

  private JsonObject updateRules(JsonObject rules) {
    var tag852Rules = rules.getJsonArray("852");
    if (tag852Rules == null) {
      return rules;
    }
    for (int i = 0; i < tag852Rules.size(); i++) {
      var entities = tag852Rules.getJsonObject(i).getJsonArray("entity");
      if (entities != null) {
        for (int j = 0; j < entities.size(); j++) {
          var rule = entities.getJsonObject(j);
          if ("callNumberTypeId".equals(rule.getString("target"))) {
            rule.put("subfield", JsonArray.of("b"));
          }
        }
      }
    }
    return rules;
  }
}
