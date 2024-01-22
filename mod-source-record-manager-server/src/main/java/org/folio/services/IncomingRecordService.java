package org.folio.services;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.IncomingRecord;

import java.util.List;
import java.util.Optional;

/**
 * {@link IncomingRecord} Service interface
 */
public interface IncomingRecordService {

  /**
   * Searches for {@link IncomingRecord} by id
   *
   * @param id incomingRecord id
   * @return future with optional incomingRecord
   */
  Future<Optional<IncomingRecord>> getById(String id, String tenantId);

  /**
   * Saves {@link IncomingRecord}s into DB
   *
   * @param incomingRecords incoming records to be saved
   * @param tenantId        tenant
   */
  void saveBatch(List<IncomingRecord> incomingRecords, String tenantId);
}
