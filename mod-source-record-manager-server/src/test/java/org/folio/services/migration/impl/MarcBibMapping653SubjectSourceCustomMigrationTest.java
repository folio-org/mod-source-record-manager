package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_BIB;
import static org.folio.TestUtil.readFileFromPath;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
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
public class MarcBibMapping653SubjectSourceCustomMigrationTest {

  private static final String TENANT_ID = "test";
  private static final String UNMODIFIED_RULES =
    "src/test/resources/org/folio/mapping/subject653source/unmodified.json";
  private static final String UPDATED_RULES =
    "src/test/resources/org/folio/mapping/subject653source/updated.json";

  private @Mock MappingRuleService mappingRuleService;
  private @InjectMocks MarcBibMapping653SubjectSourceCustomMigration migration;
  private @Captor ArgumentCaptor<String> rulesCaptor;

  @Test
  public void shouldUpdateAllSubjectSourceRulesToSourceNotSpecified() throws IOException {
    var existingRule = readFileFromPath(UNMODIFIED_RULES);
    var expectedRule = new JsonObject(readFileFromPath(UPDATED_RULES)).encode();

    when(mappingRuleService.get(eq(MARC_BIB), any()))
      .thenReturn(Future.succeededFuture(Optional.of(new JsonObject(existingRule))));
    when(mappingRuleService.internalUpdate(anyString(), eq(MARC_BIB), eq(TENANT_ID)))
      .thenReturn(Future.succeededFuture());

    migration.migrate(TENANT_ID).onComplete(ar -> {
      verify(mappingRuleService).internalUpdate(rulesCaptor.capture(), eq(MARC_BIB), eq(TENANT_ID));
      Assert.assertTrue(ar.succeeded());
      Assert.assertEquals(expectedRule, rulesCaptor.getValue());
    });
  }

  @Test
  public void shouldDoNothingIfNo653FieldPresent() {
    var rulesWithout653 = new JsonObject().put("600", new io.vertx.core.json.JsonArray());

    when(mappingRuleService.get(eq(MARC_BIB), any()))
      .thenReturn(Future.succeededFuture(Optional.of(rulesWithout653)));
    when(mappingRuleService.internalUpdate(anyString(), eq(MARC_BIB), eq(TENANT_ID)))
      .thenReturn(Future.succeededFuture());

    migration.migrate(TENANT_ID).onComplete(ar -> {
      verify(mappingRuleService).internalUpdate(rulesCaptor.capture(), eq(MARC_BIB), eq(TENANT_ID));
      Assert.assertTrue(ar.succeeded());
      Assert.assertEquals(rulesWithout653.encode(), rulesCaptor.getValue());
    });
  }

  @Test
  public void shouldDoNothingIfNoRulesExist() {
    when(mappingRuleService.get(eq(MARC_BIB), any()))
      .thenReturn(Future.succeededFuture(Optional.empty()));

    migration.migrate(TENANT_ID).onComplete(ar -> {
      verify(mappingRuleService, never()).internalUpdate(anyString(), any(), anyString());
      Assert.assertTrue(ar.succeeded());
    });
  }
}
