package org.folio.services.migration;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;

import io.vertx.core.Future;
import java.util.List;
import java.util.UUID;
import org.folio.dao.RuleMigrationChangeLogDao;
import org.folio.services.migration.impl.AuthorityMappingNameSubjectMetadataCustomMigration;
import org.folio.services.migration.impl.AuthorityRestoreAllRulesCustomMigration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CustomMigrationServiceTest {

  private static final String TENANT_ID = "test";
  private static final UUID AUTHORITY_SUBJECT_METADATA_MIGRATION_ID = UUID.randomUUID();
  private static final UUID AUTHORITY_RESTORE_MIGRATION_ID = UUID.randomUUID();

  @Mock
  private RuleMigrationChangeLogDao ruleMigrationChangeLogDao;

  @Mock
  private AuthorityRestoreAllRulesCustomMigration authorityRestoreMigration;

  @Mock
  private AuthorityMappingNameSubjectMetadataCustomMigration authoritySubjectMigration;

  private CustomMigrationService service;

  @Before
  public void setUp() {
    when(authorityRestoreMigration.getOrder()).thenReturn(2);
    when(authoritySubjectMigration.getOrder()).thenReturn(1);

    when(authorityRestoreMigration.getMigrationId()).thenReturn(AUTHORITY_RESTORE_MIGRATION_ID);
    when(authoritySubjectMigration.getMigrationId()).thenReturn(AUTHORITY_SUBJECT_METADATA_MIGRATION_ID);

    when(authorityRestoreMigration.getDescription()).thenReturn("Authority rules restore");
    when(authoritySubjectMigration.getDescription()).thenReturn("Authority subject metadata update");

    when(authorityRestoreMigration.migrate(TENANT_ID)).thenReturn(Future.succeededFuture());
    when(authoritySubjectMigration.migrate(TENANT_ID)).thenReturn(Future.succeededFuture());
    service = new CustomMigrationService(
      List.of(authoritySubjectMigration, authorityRestoreMigration), ruleMigrationChangeLogDao);
  }

  @Test
  public void shouldRunMigrationsInOrder() {
    when(ruleMigrationChangeLogDao.getMigrationIds(TENANT_ID))
      .thenReturn(Future.succeededFuture(List.of()));
    when(ruleMigrationChangeLogDao.save(any(), anyString()))
      .thenReturn(Future.succeededFuture());

    var result = service.doCustomMigrations(TENANT_ID);
    assertTrue(result.succeeded());

    InOrder inOrder = inOrder(authoritySubjectMigration, authorityRestoreMigration);
    inOrder.verify(authoritySubjectMigration).migrate(TENANT_ID);
    inOrder.verify(authorityRestoreMigration).migrate(TENANT_ID);
    verify(ruleMigrationChangeLogDao).getMigrationIds(TENANT_ID);
    verify(ruleMigrationChangeLogDao).save(authorityRestoreMigration, TENANT_ID);
    verify(ruleMigrationChangeLogDao).save(authoritySubjectMigration, TENANT_ID);
  }

  @Test
  public void shouldSkipAlreadyAppliedMigrationsAndExecuteNewOnes() {
    when(ruleMigrationChangeLogDao.getMigrationIds(TENANT_ID))
      .thenReturn(Future.succeededFuture(List.of(AUTHORITY_RESTORE_MIGRATION_ID)));
    when(ruleMigrationChangeLogDao.save(any(), anyString()))
      .thenReturn(Future.succeededFuture());

    var result = service.doCustomMigrations(TENANT_ID);
    assertTrue(result.succeeded());

    verify(authoritySubjectMigration).migrate(TENANT_ID);
    verify(ruleMigrationChangeLogDao).save(authoritySubjectMigration, TENANT_ID);
    verify(authorityRestoreMigration, never()).migrate(TENANT_ID);
    verify(ruleMigrationChangeLogDao, never()).save(authorityRestoreMigration, TENANT_ID);
  }

  @Test
  public void shouldSkipAllMigrationsWhenAllAreAlreadyApplied() {
    when(ruleMigrationChangeLogDao.getMigrationIds(TENANT_ID))
      .thenReturn(Future.succeededFuture(List.of(AUTHORITY_RESTORE_MIGRATION_ID, AUTHORITY_SUBJECT_METADATA_MIGRATION_ID)));

    var result = service.doCustomMigrations(TENANT_ID);
    assertTrue(result.succeeded());

    verify(authorityRestoreMigration, never()).migrate(TENANT_ID);
    verify(authoritySubjectMigration, never()).migrate(TENANT_ID);
    verify(ruleMigrationChangeLogDao, never()).save(authoritySubjectMigration, TENANT_ID);
    verify(ruleMigrationChangeLogDao, never()).save(authorityRestoreMigration, TENANT_ID);
  }
}

