package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.folio.rest.jaxrs.model.IncomingRecord;

import java.util.List;

/**
 * DAO interface for the {@link IncomingRecord} entity
 */
public interface IncomingRecordDao {

  /**
   * Saves {@link IncomingRecord} entities into DB
   *
   * @param incomingRecords {@link IncomingRecord} entities to save
   * @param tenantId        tenant id
   * @return future with created incomingRecords entities represented as row set
   */
  Future<List<RowSet<Row>>> saveBatch(List<IncomingRecord> incomingRecords, String tenantId);
}
