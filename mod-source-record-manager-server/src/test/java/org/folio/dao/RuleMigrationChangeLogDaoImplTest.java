package org.folio.dao;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.util.UUID;
import java.util.stream.Stream;
import org.folio.Record;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.persist.PostgresClient;
import org.folio.services.migration.CustomMigration;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RuleMigrationChangeLogDaoImplTest {

  private static final UUID MIGRATION_ID = UUID.randomUUID();
  private static final String TENANT_ID = "test";
  private static final String ERROR_MESSAGE = "Error occurred";

  @InjectMocks
  private RuleMigrationChangeLogDaoImpl changeLogDao;

  @Mock
  private PostgresClientFactory postgresClientFactory;

  @Mock
  private PostgresClient pgClient;

  @Mock
  private CustomMigration customMigration;

  @Mock
  private RowSet<Row> rowSet;

  @Before
  public void setUp() {
    when(postgresClientFactory.createInstance(TENANT_ID)).thenReturn(pgClient);
    when(customMigration.getMigrationId()).thenReturn(MIGRATION_ID);
    when(customMigration.getRecordType()).thenReturn(Record.RecordType.MARC_AUTHORITY);
    when(customMigration.getDescription()).thenReturn("description");
  }

  @Test
  public void getMigrationIds_shouldReturnEmptyList_whenNoRows() {
    when(rowSet.spliterator()).thenReturn(Stream.<Row>empty().spliterator());

    doAnswer(invocation -> {
      Promise<RowSet<Row>> promise = invocation.getArgument(2);
      promise.complete(rowSet);
      return null;
    }).when(pgClient).selectRead(anyString(), any(), any());

    changeLogDao.getMigrationIds(TENANT_ID).onComplete(ar -> {
      assertTrue(ar.succeeded());
      assertNotNull(ar.result());
      assertTrue(ar.result().isEmpty());
    });
  }

  @Test
  public void getMigrationIds_shouldReturnList() {
    var row = mock(Row.class);
    when(row.getUUID("migration_id")).thenReturn(MIGRATION_ID);
    when(rowSet.spliterator()).thenReturn(Stream.of(row).spliterator());

    doAnswer(inv -> {
      Promise<RowSet<Row>> p = inv.getArgument(2);
      p.complete(rowSet);
      return null;
    }).when(pgClient).selectRead(anyString(), any(), any());

    changeLogDao.getMigrationIds(TENANT_ID).onComplete(ar -> {
      assertTrue(ar.succeeded());
      assertNotNull(ar.result());
      assertEquals(1, ar.result().size());
      assertEquals(MIGRATION_ID, ar.result().getFirst());
    });
  }

  @Test
  public void getMigrationIds_shouldFail_whenSelectFails() {
    doAnswer(inv -> {
      Promise<RowSet<Row>> p = inv.getArgument(2);
      p.fail(new RuntimeException(ERROR_MESSAGE));
      return null;
    }).when(pgClient).selectRead(anyString(), any(), any());

    changeLogDao.getMigrationIds(TENANT_ID).onComplete(ar -> {
      assertTrue(ar.failed());
      assertEquals(ERROR_MESSAGE, ar.cause().getMessage());
    });
  }

  @Test
  public void save_shouldSucceed_whenExecuteSucceeds() {
    when(pgClient.execute(anyString(), any(Tuple.class)))
      .thenReturn(Future.succeededFuture(rowSet));

    changeLogDao.save(customMigration, TENANT_ID).onComplete(ar -> {
      assertTrue(ar.succeeded());
    });
  }

  @Test
  public void save_shouldFail_whenExecuteReturnsFailedFuture() {
    when(pgClient.execute(anyString(), any(Tuple.class)))
      .thenReturn(Future.failedFuture(new RuntimeException(ERROR_MESSAGE)));

    changeLogDao.save(customMigration, TENANT_ID).onComplete(ar -> {
      assertTrue(ar.failed());
      assertEquals(ERROR_MESSAGE, ar.cause().getMessage());
    });
  }
}
