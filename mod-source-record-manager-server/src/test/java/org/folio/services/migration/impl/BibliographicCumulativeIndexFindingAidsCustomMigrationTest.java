package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_BIB;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import java.util.Optional;
import org.folio.services.MappingRuleService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BibliographicCumulativeIndexFindingAidsCustomMigrationTest {

  private static final String TENANT_ID = "test";

  private @Mock MappingRuleService mappingRuleService;
  private @InjectMocks BibliographicCumulativeIndexFindingAidsCustomMigration migration;
  private @Captor ArgumentCaptor<String> rulesCaptor;

  @Test
  public void shouldDoMigration() {
    var existedRules = """
      {
        "555": [
            {
              "entity": [
                {
                  "target": "notes.instanceNoteTypeId",
                  "description": "Instance note type id",
                  "subfield": [
                    "a",
                    "b",
                    "c",
                    "d",
                    "u",
                    "3"
                  ],
                  "applyRulesOnConcatenatedData": true,
                  "rules": [
                    {
                      "conditions": [
                        {
                          "type": "set_note_type_id",
                          "parameter": {
                            "name": "Cumulative Index / Finding Aides notes"
                          }
                        }
                      ]
                    }
                  ]
                }
              ]
            }
        ]
      }
      """;
    var expectedRules = """
      {
        "555": [
            {
              "entity": [
                {
                  "target": "notes.instanceNoteTypeId",
                  "description": "Instance note type id",
                  "subfield": [
                    "a",
                    "b",
                    "c",
                    "d",
                    "u",
                    "3"
                  ],
                  "applyRulesOnConcatenatedData": true,
                  "rules": [
                    {
                      "conditions": [
                        {
                          "type": "set_note_type_id",
                          "parameter": {
                            "name": "Cumulative Index / Finding Aids notes"
                          }
                        }
                      ]
                    }
                  ]
                }
              ]
            }
        ]
      }
      """;
    when(mappingRuleService.get(eq(MARC_BIB), any())).thenReturn(Future.succeededFuture(
      Optional.of(new JsonObject(existedRules))
    ));

    when(mappingRuleService.internalUpdate(any(), eq(MARC_BIB), any())).thenReturn(Future.succeededFuture());

    migration.migrate(TENANT_ID).onComplete(ar -> {
      verify(mappingRuleService).internalUpdate(rulesCaptor.capture(), eq(MARC_BIB), eq(TENANT_ID));
      Assert.assertTrue(ar.succeeded());
      Assert.assertEquals(Json.decodeValue(expectedRules), Json.decodeValue(rulesCaptor.getValue()));
    });
  }

  @Test
  public void shouldDoNothingIfNoRulesExist() {
    when(mappingRuleService.get(eq(MARC_BIB), any())).thenReturn(Future.succeededFuture(Optional.empty()));

    migration.migrate(TENANT_ID).onComplete(ar -> {
      verify(mappingRuleService, never()).update(any(), any(), any());
      Assert.assertTrue(ar.succeeded());
    });
  }
}