package org.folio.services;

import io.vertx.core.Future;
import org.folio.dao.SourceRecordStateDao;
import org.folio.rest.jaxrs.model.SourceRecordState;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
public class SourceRecordStateServiceImpl implements SourceRecordStateService {

  private SourceRecordStateDao sourceRecordStateDao;

  public SourceRecordStateServiceImpl(@Autowired SourceRecordStateDao sourceRecordStateDao) {
    this.sourceRecordStateDao = sourceRecordStateDao;
  }

  @Override
  public Future<Optional<SourceRecordState>> get(String sourceRecordId, String tenantId) {
    return sourceRecordStateDao.get(sourceRecordId, tenantId);
  }

  @Override
  public Future<String> save(SourceRecordState state, String tenantId) {
    return sourceRecordStateDao.save(state, tenantId);
  }

  @Override
  public Future<SourceRecordState> updateState(String sourceId, SourceRecordState.RecordState recordState, String tenantId) {
    return sourceRecordStateDao.updateState(sourceId, recordState, tenantId);
  }
}
