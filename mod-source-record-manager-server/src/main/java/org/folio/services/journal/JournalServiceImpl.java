package org.folio.services.journal;

import io.vertx.core.Vertx;
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

  @Override
  public void save(JsonObject jsonJournalRecord, String tenantId) {
    JournalRecord journalRecord = jsonJournalRecord.mapTo(JournalRecord.class);
    journalRecordDao.save(journalRecord, tenantId);
  }

}
