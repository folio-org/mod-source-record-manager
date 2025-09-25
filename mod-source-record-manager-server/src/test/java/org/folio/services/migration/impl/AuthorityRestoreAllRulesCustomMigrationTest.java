package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import org.folio.services.MappingRuleService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class AuthorityRestoreAllRulesCustomMigrationTest {
  private static final String TENANT_ID = "test";

  private @Mock MappingRuleService mappingRuleService;
  private @InjectMocks AuthorityRestoreAllRulesCustomMigration migration;

  @Test
  public void shouldSucceed_whenRestoringAllAuthorityRules() {
    when(mappingRuleService.restore(MARC_AUTHORITY, TENANT_ID)).thenReturn(Future.succeededFuture());

    migration.migrate(TENANT_ID).onComplete(ar -> {
      verify(mappingRuleService).restore(MARC_AUTHORITY, TENANT_ID);
      Assert.assertTrue(ar.succeeded());
    });
  }

  @Test
  public void shouldFail_whenRestoringAuthorityRulesFails() {
    when(mappingRuleService.restore(MARC_AUTHORITY, TENANT_ID)).thenReturn(Future.failedFuture("Error"));

    migration.migrate(TENANT_ID).onComplete(ar -> {
      Assert.assertTrue(ar.failed());
      Assert.assertEquals("Error", ar.cause().getMessage());
    });
  }
}

