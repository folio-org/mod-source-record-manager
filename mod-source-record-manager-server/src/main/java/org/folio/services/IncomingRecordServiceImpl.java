package org.folio.services;

import io.vertx.core.Future;
import java.util.List;
import java.util.Optional;
import org.folio.dao.IncomingRecordDao;
import org.folio.rest.jaxrs.model.IncomingRecord;
import org.springframework.stereotype.Service;

@Service
public class IncomingRecordServiceImpl implements IncomingRecordService {

  private final IncomingRecordDao incomingRecordDao;

  public IncomingRecordServiceImpl(IncomingRecordDao incomingRecordDao) { this.incomingRecordDao = incomingRecordDao; }

  @Override
  public Future<Optional<IncomingRecord>> getById(String id, String tenantId) {
    return incomingRecordDao.getById(id, tenantId);
  }

  @Override
  public void saveBatch(List<IncomingRecord> incomingRecords, String tenantId) {
    incomingRecordDao.saveBatch(incomingRecords, tenantId);
  }
}
