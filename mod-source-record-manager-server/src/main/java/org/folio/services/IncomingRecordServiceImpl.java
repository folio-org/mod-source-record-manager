package org.folio.services;

import io.vertx.core.Future;
import org.folio.dao.IncomingRecordDao;
import org.folio.rest.jaxrs.model.IncomingRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

@Service
public class IncomingRecordServiceImpl implements IncomingRecordService {

  @Autowired
  private IncomingRecordDao incomingRecordDao;

  @Override
  public Future<Optional<IncomingRecord>> getById(String id, String tenantId) {
    return incomingRecordDao.getById(id, tenantId);
  }

  @Override
  public void saveBatch(List<IncomingRecord> incomingRecords, String tenantId) {
    incomingRecordDao.saveBatch(incomingRecords, tenantId);
  }
}
