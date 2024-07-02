package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;
import static org.folio.TestUtil.readFileFromPath;
import static org.mockito.ArgumentMatchers.any;
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
public class AuthorityMappingNameSubjectMetadataCustomMigrationTest {

  private static final String TENANT_ID = "test";
  private static final String RULES_PATH = "src/test/resources/org/folio/mapping/";
  private static final String OLD_RULES = RULES_PATH + "rulesAuthoritySubjectMetadataOld.json";
  private static final String NEW_RULES = RULES_PATH + "rulesAuthoritySubjectMetadataNew.json";

  private @Mock MappingRuleService mappingRuleService;
  private @InjectMocks AuthorityMappingNameSubjectMetadataCustomMigration migration;
  private @Captor ArgumentCaptor<String> rulesCaptor;

  @Test
  public void shouldUpdateNameSubjectMetadataAuthorityRules_noUpdateToNameTitleRules() throws IOException {
    var existedRules = readFileFromPath(OLD_RULES);
    var expectedRules = new JsonObject(readFileFromPath(NEW_RULES)).encode();
    when(mappingRuleService.get(MARC_AUTHORITY, TENANT_ID)).thenReturn(Future.succeededFuture(
      Optional.of(new JsonObject(existedRules))
    ));

    when(mappingRuleService.internalUpdate(any(), eq(MARC_AUTHORITY), any())).thenReturn(Future.succeededFuture());

    migration.migrate(TENANT_ID).onComplete(ar -> {
      verify(mappingRuleService).internalUpdate(rulesCaptor.capture(), eq(MARC_AUTHORITY), eq(TENANT_ID));
      Assert.assertTrue(ar.succeeded());
      Assert.assertEquals(expectedRules, rulesCaptor.getValue());
    });
  }

  @Test
  public void shouldDoNothingIfNoRulesExist() {
    when(mappingRuleService.get(eq(MARC_AUTHORITY), any())).thenReturn(Future.succeededFuture(Optional.empty()));

    migration.migrate(TENANT_ID).onComplete(ar -> {
      verify(mappingRuleService, never()).update(any(), any(), any());
      verify(mappingRuleService, never()).internalUpdate(any(), any(), any());
      Assert.assertTrue(ar.succeeded());
    });
  }
}
