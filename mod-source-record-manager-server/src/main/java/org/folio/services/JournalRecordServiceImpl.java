package org.folio.services;

import io.vertx.core.Future;
import org.folio.dao.JournalRecordDao;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Implementation of the JournalRecordService.
 *
 * @see JournalRecordService
 */
@Service
public class JournalRecordServiceImpl implements JournalRecordService {

  @Autowired
  private JournalRecordDao journalRecordDao;

  @Override
  public Future<Boolean> deleteByJobExecutionId(String jobExecutionId, String tenantId) {
    return journalRecordDao.deleteByJobExecutionId(jobExecutionId, tenantId);
  }
}
