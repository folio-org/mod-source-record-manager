package org.folio.services.journal;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.dao.JournalRecordDao;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

public class JournalServiceImpl implements JournalService {

  @Autowired
  private JournalRecordDao journalRecordDao;

  public JournalServiceImpl() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

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
    for (int i = 0; i < journalRecords.size(); i++) {
      JournalRecord journalRecord = journalRecords.getJsonObject(i).mapTo(JournalRecord.class);
      journalRecordDao.save(journalRecord, tenantId);
    }
  }
}
