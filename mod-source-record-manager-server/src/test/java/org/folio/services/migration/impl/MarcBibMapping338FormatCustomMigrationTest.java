package org.folio.services.migration.impl;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import org.folio.services.MappingRuleService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.IOException;
import java.util.Optional;

import static org.folio.Record.RecordType.MARC_BIB;
import static org.folio.TestUtil.readFileFromPath;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MarcBibMapping338FormatCustomMigrationTest {

  private static final String TENANT_ID = "test";
  private static final String EXISTING_RULE = "src/test/resources/org/folio/mapping/format/unmodified.json";
  private static final String EXPECTED_RULE = "src/test/resources/org/folio/mapping/format/updated.json";

  private @Mock MappingRuleService mappingRuleService;
  private @InjectMocks MarcBibMapping338FormatCustomMigration migration;
  private @Captor ArgumentCaptor<String> rulesCaptor;

  @Test
  public void shouldUpdateFormatRule() throws IOException {
    var existingRule = readFileFromPath(EXISTING_RULE);
    var expectedRule = new JsonObject(readFileFromPath(EXPECTED_RULE)).encode();

    when(mappingRuleService.get(eq(MARC_BIB), any())).thenReturn(Future.succeededFuture(
      Optional.of(new JsonObject(existingRule))));
    when(mappingRuleService.internalUpdate(anyString(), eq(MARC_BIB), eq(TENANT_ID)))
      .thenReturn(Future.succeededFuture());

    migration.migrate(TENANT_ID).onComplete(ar -> {
      verify(mappingRuleService).internalUpdate(rulesCaptor.capture(), eq(MARC_BIB), eq(TENANT_ID));
      Assert.assertTrue(ar.succeeded());
      Assert.assertEquals(expectedRule, rulesCaptor.getValue());
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
