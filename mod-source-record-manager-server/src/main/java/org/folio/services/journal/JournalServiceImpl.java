package org.folio.services.journal;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang.StringUtils;
import org.folio.dao.JournalRecordDao;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

@Service("journalService")
public class JournalServiceImpl implements JournalService, BatchJournalService {

  private JournalRecordDao journalRecordDao;

  @Autowired
  public JournalServiceImpl(JournalRecordDao journalRecordDao) {
    this.journalRecordDao = journalRecordDao;
  }

  @Override
  public void save(JsonObject journalRecord, String tenantId) {
    JournalRecord record = journalRecord.mapTo(JournalRecord.class);
    journalRecordDao.save(record, tenantId);
  }

  @Override
  public void saveBatch(JsonArray journalRecords, String tenantId) {
    List<JournalRecord> journalRecordList = new ArrayList<>();
    for (int i = 0; i < journalRecords.size(); i++) {
      JournalRecord journalRecord = journalRecords.getJsonObject(i).mapTo(JournalRecord.class);
      journalRecord.setId(UUID.randomUUID().toString());
      journalRecordList.add(journalRecord);
    }
    saveBatchWithResponse(journalRecordList, tenantId, ar -> {});
  }

  @Override
  public void saveBatchWithResponse(Collection<JournalRecord> journalRecords, String tenantId, Handler<AsyncResult<Void>> resultHandler) {
    journalRecords.forEach(journalRecord -> {
      if (StringUtils.isEmpty(journalRecord.getId())) {
        journalRecord.setId(UUID.randomUUID().toString());
      }
    });
    journalRecordDao.saveBatch(journalRecords, tenantId)
      .map((Void) null)
      .onComplete(resultHandler);
  }

  @Override
  public JournalService getJournalService() {
    return this;
  }
}
