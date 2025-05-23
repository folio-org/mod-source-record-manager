package org.folio.services.migration.impl;

import static org.folio.Record.RecordType.MARC_AUTHORITY;
import static org.folio.TestUtil.readFileFromPath;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Optional;
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

@RunWith(MockitoJUnitRunner.class)
public class AuthorityMappingChronTermCustomMigrationTest {

  private static final String TENANT_ID = "test";
  private static final String PATH = "src/test/resources/org/folio/mapping/chronTerm/";

  private @Mock MappingRuleService mappingRuleService;
  private @InjectMocks AuthorityMappingChronTermCustomMigration migration;
  private @Captor ArgumentCaptor<String> rulesCaptor;

  @Test
  public void shouldAddChronTermFieldsToMarcAuthorityRules() throws IOException {
    var existedJson = readFileFromPath(PATH + "rulesAuthorityChronTermExisted.json");
    var expectedJson = readFileFromPath(PATH + "rulesAuthorityChronTermExpected.json");
    var expectedRules = new JsonObject(expectedJson).encode();

    when(mappingRuleService.get(MARC_AUTHORITY, TENANT_ID))
      .thenReturn(Future.succeededFuture(Optional.of(new JsonObject(existedJson))));
    when(mappingRuleService.internalUpdate(anyString(), eq(MARC_AUTHORITY), eq(TENANT_ID)))
      .thenReturn(Future.succeededFuture());

    migration.migrate(TENANT_ID).onComplete(ar -> {
      verify(mappingRuleService).internalUpdate(rulesCaptor.capture(), eq(MARC_AUTHORITY), eq(TENANT_ID));
      Assert.assertTrue(ar.succeeded());
      Assert.assertEquals(expectedRules, rulesCaptor.getValue());
    });
  }
}
