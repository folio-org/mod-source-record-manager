package org.folio.services;

import org.folio.dao.IncomingRecordDao;
import org.folio.rest.jaxrs.model.IncomingRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class IncomingRecordServiceImpl implements IncomingRecordService {

  @Autowired
  private IncomingRecordDao incomingRecordDao;

  @Override
  public void saveBatch(List<IncomingRecord> incomingRecords, String tenantId) {
    incomingRecordDao.saveBatch(incomingRecords, tenantId);
  }
}
