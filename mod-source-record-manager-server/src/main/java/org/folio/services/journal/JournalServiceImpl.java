package org.folio.services.journal;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.dao.JournalRecordDao;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service("journalService")
public class JournalServiceImpl implements JournalService {

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
    journalRecordDao.saveBatch(journalRecordList, tenantId);
  }
}
