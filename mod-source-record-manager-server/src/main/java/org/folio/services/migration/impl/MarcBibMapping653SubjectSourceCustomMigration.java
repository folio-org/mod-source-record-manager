package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_BIB;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.UUID;
import lombok.extern.log4j.Log4j2;
import org.folio.services.MappingRuleService;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class MarcBibMapping653SubjectSourceCustomMigration extends BaseMappingRulesMigration {

  private static final int ORDER = 17;
  private static final UUID MIGRATION_ID = UUID.fromString("225672ed-94f0-4fcb-8074-899db54bdbf8");
  private static final String DESCRIPTION =
    "MARC Bib mapping rules: set Subject source to 'Source not specified' for all 653 field entries";
  private static final String RULE_TAG = "653";
  private static final String SUBJECTS_SOURCE_ID = "subjects.sourceId";
  private static final String SOURCE_NOT_SPECIFIED = "Source not specified";

  protected MarcBibMapping653SubjectSourceCustomMigration(MappingRuleService mappingRuleService) {
    super(MARC_BIB, ORDER, DESCRIPTION, MIGRATION_ID, mappingRuleService);
  }

  @Override
  protected String updateRules(JsonObject rules) {
    var ruleArray = rules.getJsonArray(RULE_TAG);
    if (ruleArray == null) {
      return rules.encode();
    }

    var sourceNotSpecifiedEntity = buildSourceNotSpecifiedEntity();

    for (int i = 0; i < ruleArray.size(); i++) {
      var entry = ruleArray.getJsonObject(i);
      var entityList = entry.getJsonArray("entity");
      if (entityList == null) {
        continue;
      }

      int srcIdx = -1;
      for (int j = 0; j < entityList.size(); j++) {
        if (SUBJECTS_SOURCE_ID.equals(entityList.getJsonObject(j).getString("target"))) {
          srcIdx = j;
          break;
        }
      }

      if (srcIdx >= 0) {
        entityList.set(srcIdx, sourceNotSpecifiedEntity);
      } else {
        entityList.add(sourceNotSpecifiedEntity);
      }
    }

    log.info("Updated 653 subject source rules to '{}'", SOURCE_NOT_SPECIFIED);
    return rules.encode();
  }

  private JsonObject buildSourceNotSpecifiedEntity() {
    var subfields = new JsonArray()
      .add("a").add("b").add("c").add("d").add("e").add("f").add("g").add("h")
      .add("j").add("k").add("l").add("m").add("n").add("o").add("p").add("q")
      .add("r").add("s").add("t").add("u").add("v").add("x").add("y").add("z");

    var condition = new JsonObject()
      .put("type", "set_subject_source_id")
      .put("parameter", new JsonObject().put("name", SOURCE_NOT_SPECIFIED));

    var rule = new JsonObject().put("conditions", JsonArray.of(condition));

    return new JsonObject()
      .put("target", SUBJECTS_SOURCE_ID)
      .put("description", "Subject source")
      .put("subfield", subfields)
      .put("applyRulesOnConcatenatedData", true)
      .put("rules", JsonArray.of(rule));
  }
}
