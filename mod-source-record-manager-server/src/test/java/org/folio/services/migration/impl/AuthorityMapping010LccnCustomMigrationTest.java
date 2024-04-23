package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;
import static org.folio.Record.RecordType.MARC_HOLDING;
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
public class AuthorityMapping010LccnCustomMigrationTest {

  private static final String TENANT_ID = "test";

  private @Mock MappingRuleService mappingRuleService;
  private @InjectMocks AuthorityMapping010LccnCustomMigration migration;
  private @Captor ArgumentCaptor<String> rulesCaptor;

  @Test
  public void shouldUpdateLccnAndCancelledLccnAuthorityRules() {
    var existedRules = "{\"010\":[{\"entity\":[{" +
      "\"target\":\"identifiers.identifierTypeId\",\"description\":\"Identifier Type for LCCN\"," +
      "\"subfield\":[\"a\",\"z\"],\"rules\":[{\"conditions\":[{\"type\":" +
      "\"set_identifier_type_id_by_name\",\"parameter\":{\"name\":\"LCCN\"}}]}]},{" +
      "\"target\":\"identifiers.value\",\"description\":\"Library of Congress Control Number\"," +
      "\"subfield\":[\"a\",\"z\"],\"rules\":[{\"conditions\":[{\"type\":\"trim\"}]}]}]}]}";
    var expectedRules = "{\"010\":[{\"entity\":[{" +
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
    when(mappingRuleService.get(eq(MARC_AUTHORITY), any())).thenReturn(Future.succeededFuture(
      Optional.of(new JsonObject(existedRules))
    ));

    when(mappingRuleService.update(any(), eq(MARC_AUTHORITY), any())).thenReturn(Future.succeededFuture());

    migration.migrate(TENANT_ID).onComplete(ar -> {
      verify(mappingRuleService).update(rulesCaptor.capture(), eq(MARC_AUTHORITY), eq(TENANT_ID));
      Assert.assertTrue(ar.succeeded());
      Assert.assertEquals(expectedRules, rulesCaptor.getValue());
    });
  }

  @Test
  public void shouldDoNothingIfNoRulesExist() {
    when(mappingRuleService.get(eq(MARC_AUTHORITY), any())).thenReturn(Future.succeededFuture(Optional.empty()));

    migration.migrate(TENANT_ID).onComplete(ar -> {
      verify(mappingRuleService, never()).update(any(), any(), any());
      Assert.assertTrue(ar.succeeded());
    });
  }
}
