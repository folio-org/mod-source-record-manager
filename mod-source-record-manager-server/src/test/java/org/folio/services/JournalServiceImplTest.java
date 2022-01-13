package org.folio.services;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.dao.JournalRecordDao;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.services.journal.JournalService;
import org.folio.services.journal.JournalServiceImpl;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Date;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class JournalServiceImplTest {

  private static final String TENANT_ID = "diku";

  @Mock
  private JournalRecordDao journalRecordDao;

  @InjectMocks
  private JournalService journalService = new JournalServiceImpl(journalRecordDao);

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void shouldCallSaveOnce() {
    JournalRecord journalRecord = new JournalRecord()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withSourceId(UUID.randomUUID().toString())
      .withSourceRecordOrder(1)
      .withEntityType(JournalRecord.EntityType.INSTANCE)
      .withEntityId(UUID.randomUUID().toString())
      .withEntityHrId(UUID.randomUUID().toString())
      .withActionType(JournalRecord.ActionType.CREATE)
      .withActionDate(new Date())
      .withActionStatus(JournalRecord.ActionStatus.COMPLETED);

    JsonObject jsonJournalRecord = JsonObject.mapFrom(journalRecord);

    when(journalRecordDao.save(any(JournalRecord.class), eq(TENANT_ID))).thenReturn(Future.succeededFuture());

    journalService.save(jsonJournalRecord, TENANT_ID);

    verify(journalRecordDao, times(1)).save(any(JournalRecord.class), eq(TENANT_ID));
  }

  @Test
  public void shouldCallSaveBatchOnce() {
    JournalRecord journalRecord = new JournalRecord()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withSourceId(UUID.randomUUID().toString())
      .withSourceRecordOrder(1)
      .withEntityType(JournalRecord.EntityType.INSTANCE)
      .withEntityId(UUID.randomUUID().toString())
      .withEntityHrId(UUID.randomUUID().toString())
      .withActionType(JournalRecord.ActionType.CREATE)
      .withActionDate(new Date())
      .withActionStatus(JournalRecord.ActionStatus.COMPLETED);

    JsonObject jsonJournalRecord = JsonObject.mapFrom(journalRecord);

    JsonArray jsonArray = new JsonArray()
      .add(jsonJournalRecord)
      .add(jsonJournalRecord)
      .add(jsonJournalRecord);

    when(journalRecordDao.saveBatch(any(JsonArray.class), eq(TENANT_ID))).thenReturn(Future.succeededFuture());

    journalService.saveBatch(jsonArray, TENANT_ID);

    verify(journalRecordDao, times(0)).save(any(JournalRecord.class), eq(TENANT_ID));
    verify(journalRecordDao, times(1)).saveBatch(any(JsonArray.class), eq(TENANT_ID));
  }
}
