package org.folio.services.migration.impl;

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
public class HoldingsMapping852CallNumberTypeCustomMigrationTest {

  private static final String TENANT_ID = "test";

  private @Mock MappingRuleService mappingRuleService;
  private @InjectMocks HoldingsMapping852CallNumberTypeCustomMigration migration;
  private @Captor ArgumentCaptor<String> rulesCaptor;

  @Test
  public void shouldUpdateCallNumberTypeRule() {
    var existedRules = "{\"852\":[{\"entity\":[{\"target\":\"callNumberTypeId\","
      + "\"description\":\"Call number type\",\"subfield\":[],"
      + "\"rules\":[{\"conditions\":[{\"type\":\"set_call_number_type_id\"}]}]},"
      + "{\"target\":\"permanentLocationId\",\"description\":"
      + "\"Shelving location\",\"subfield\":[\"b\"],\"rules\":"
      + "[{\"conditions\":[{\"type\":\"set_permanent_location_id\"}]}]}]}]}";
    var expectedRules = "{\"852\":[{\"entity\":[{\"target\":\"callNumberTypeId\","
      + "\"description\":\"Call number type\",\"subfield\":[\"b\"],"
      + "\"rules\":[{\"conditions\":[{\"type\":\"set_call_number_type_id\"}]}]},"
      + "{\"target\":\"permanentLocationId\",\"description\":"
      + "\"Shelving location\",\"subfield\":[\"b\"],\"rules\":"
      + "[{\"conditions\":[{\"type\":\"set_permanent_location_id\"}]}]}]}]}";
    when(mappingRuleService.get(eq(MARC_HOLDING), any())).thenReturn(Future.succeededFuture(
      Optional.of(new JsonObject(existedRules))
    ));

    when(mappingRuleService.update(any(), eq(MARC_HOLDING), any())).thenReturn(Future.succeededFuture());

    migration.migrate(TENANT_ID).onComplete(ar -> {
      verify(mappingRuleService).update(rulesCaptor.capture(), eq(MARC_HOLDING), eq(TENANT_ID));
      Assert.assertTrue(ar.succeeded());
      Assert.assertEquals(expectedRules, rulesCaptor.getValue());
    });
  }

  @Test
  public void shouldDoNothingIfNoRulesExist() {
    when(mappingRuleService.get(eq(MARC_HOLDING), any())).thenReturn(Future.succeededFuture(Optional.empty()));

    migration.migrate(TENANT_ID).onComplete(ar -> {
      verify(mappingRuleService, never()).update(any(), any(), any());
      Assert.assertTrue(ar.succeeded());
    });
  }
}
