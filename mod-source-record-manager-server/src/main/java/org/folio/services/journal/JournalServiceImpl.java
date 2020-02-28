package org.folio.services.journal;

import org.folio.dao.JournalRecordDao;
import org.folio.rest.jaxrs.model.JournalRecord;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class JournalServiceImpl implements JournalService {

  private static final Logger LOGGER = LoggerFactory.getLogger(JournalServiceImpl.class);

  @Autowired
  private JournalRecordDao journalRecordDao;

  public JournalServiceImpl() {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
  }

  @Override
  public void saveJournalRecord(JsonObject journalRecord, String tenantId) {
    JournalRecord record = journalRecord.mapTo(JournalRecord.class);
    Future<String> savedId = journalRecordDao.save(record, tenantId);
    LOGGER.info("journal record with id: {} was saved", savedId.result());
  }

  @Override
  public void saveBatchJournalRecords(JsonArray journalRecords, String tenantId) {
    for (int i = 0; i < journalRecords.size(); i++) {
      JournalRecord journalRecord = journalRecords.getJsonObject(i).mapTo(JournalRecord.class);
      journalRecordDao.save(journalRecord, tenantId);
    }
  }
}
