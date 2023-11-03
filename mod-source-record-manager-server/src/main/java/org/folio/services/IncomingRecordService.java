package org.folio.services;

import org.folio.rest.jaxrs.model.IncomingRecord;

import java.util.List;

/**
 * {@link IncomingRecord} Service interface
 */
public interface IncomingRecordService {

  /**
   * Saves {@link IncomingRecord}s into DB
   *
   * @param incomingRecords incoming records to be saved
   * @param tenantId        tenant
   */
  void saveBatch(List<IncomingRecord> incomingRecords, String tenantId);
}
