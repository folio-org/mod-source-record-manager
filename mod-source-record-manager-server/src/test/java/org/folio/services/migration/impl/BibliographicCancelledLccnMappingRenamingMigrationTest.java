package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_BIB;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
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
public class BibliographicCancelledLccnMappingRenamingMigrationTest {

  private static final String TENANT_ID = "test";

  private @Mock MappingRuleService mappingRuleService;
  private @InjectMocks BibliographicCancelledLccnMappingRenamingMigration migration;
  private @Captor ArgumentCaptor<String> rulesCaptor;

  @Test
  public void shouldRenameCancelledLCCN() {
    var existedRules = "{\"010\":[{\"entity\":[{" +
      "\"target\":\"identifiers.identifierTypeId\",\"description\":\"Identifier Type for LCCN\"," +
      "\"subfield\":[\"a\"],\"rules\":[{\"conditions\":[{\"type\":\"set_identifier_type_id_by_name\"," +
      "\"parameter\":{\"name\":\"LCCN\"}}]}]},{" +
      "\"target\":\"identifiers.value\",\"description\":\"Library of Congress Control Number\"," +
      "\"subfield\":[\"a\"],\"rules\":[{\"conditions\":[{\"type\":\"trim\"}]}]},{" +
      "\"target\":\"identifiers.identifierTypeId\",\"description\":\"Identifier Type for Cancelled LCCN\"," +
      "\"subfield\":[\"z\"],\"rules\":[{\"conditions\":[{\"type\":\"set_identifier_type_id_by_name\"," +
      "\"parameter\":{\"name\":\"Cancelled LCCN\"}}]}]},{" +
      "\"target\":\"identifiers.value\",\"description\":\"Cancelled Library of Congress Control Number\"," +
      "\"subfield\":[\"z\"],\"rules\":[{\"conditions\":[{\"type\":\"trim\"}]}]}]}]}";
    var expectedRules = "{\"010\":[{\"entity\":[{" +
      "\"target\":\"identifiers.identifierTypeId\",\"description\":\"Identifier Type for LCCN\"," +
      "\"subfield\":[\"a\"],\"rules\":[{\"conditions\":[{\"type\":\"set_identifier_type_id_by_name\"," +
      "\"parameter\":{\"name\":\"LCCN\"}}]}]},{" +
      "\"target\":\"identifiers.value\",\"description\":\"Library of Congress Control Number\"," +
      "\"subfield\":[\"a\"],\"rules\":[{\"conditions\":[{\"type\":\"trim\"}]}]},{" +
      "\"target\":\"identifiers.identifierTypeId\",\"description\":\"Identifier Type for Canceled LCCN\"," +
      "\"subfield\":[\"z\"],\"rules\":[{\"conditions\":[{\"type\":\"set_identifier_type_id_by_name\"," +
      "\"parameter\":{\"name\":\"Canceled LCCN\"}}]}]},{" +
      "\"target\":\"identifiers.value\",\"description\":\"Cancelled Library of Congress Control Number\"," +
      "\"subfield\":[\"z\"],\"rules\":[{\"conditions\":[{\"type\":\"trim\"}]}]}]}]}";
    when(mappingRuleService.get(eq(MARC_BIB), any())).thenReturn(Future.succeededFuture(
      Optional.of(new JsonObject(existedRules))
    ));

    when(mappingRuleService.internalUpdate(any(), eq(MARC_BIB), any())).thenReturn(Future.succeededFuture());

    migration.migrate(TENANT_ID).onComplete(ar -> {
      verify(mappingRuleService).internalUpdate(rulesCaptor.capture(), eq(MARC_BIB), eq(TENANT_ID));
      Assert.assertTrue(ar.succeeded());
      Assert.assertEquals(expectedRules, rulesCaptor.getValue());
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