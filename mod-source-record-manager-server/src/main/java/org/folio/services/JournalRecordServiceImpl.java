package org.folio.services;

import io.vertx.core.Future;
import org.folio.dao.JournalRecordDao;
import org.folio.rest.jaxrs.model.JobExecutionLogDto;
import org.folio.rest.jaxrs.model.JobLogEntryDtoCollection;
import org.folio.rest.jaxrs.model.JournalRecordCollection;
import org.folio.rest.jaxrs.model.RecordProcessingLogDto;
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

  @Override
  public Future<JobExecutionLogDto> getJobExecutionLogDto(String jobExecutionId, String tenantId) {
    return journalRecordDao.getJobExecutionLogDto(jobExecutionId, tenantId);
  }

  @Override
  public Future<JournalRecordCollection> getJobExecutionJournalRecords(String jobExecutionId, String sortBy, String order, String tenantId) {
    return journalRecordDao.getByJobExecutionId(jobExecutionId, sortBy, order, tenantId)
      .map(journalRecords -> new JournalRecordCollection()
        .withJournalRecords(journalRecords)
        .withTotalRecords(journalRecords.size()));
  }

  @Override
  public Future<JobLogEntryDtoCollection> getJobLogEntryDtoCollection(String jobExecutionId, String sortBy, String order, int limit, int offset, String tenantId) {
    return journalRecordDao.getJobLogEntryDtoCollection(jobExecutionId, sortBy, order, limit, offset, tenantId);
  }

  @Override
  public Future<RecordProcessingLogDto> getRecordProcessingLogDto(String jobExecutionId, String recordId, String tenantId) {
    return journalRecordDao.getRecordProcessingLogDto(jobExecutionId, recordId, tenantId);
  }
}
