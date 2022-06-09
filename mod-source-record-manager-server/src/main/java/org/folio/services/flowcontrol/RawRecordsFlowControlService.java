package org.folio.services.flowcontrol;

/**
 * Service to implement flow control logic to be able to import OCLC files in between imports of huge files.
 * It necessary to not wait for importing 1 record file until other big files in progress.
 * It is suitable for not only OCLC import, but also other import types that can skip DI_RAW_RECORDS_CHUNK_READ stage
 * and pushing messages directly to subsequent topics in the flow.
 */
public interface RawRecordsFlowControlService {

  /**
   * Tracks each DI_RAW_RECORDS_CHUNK_READ event, this method can also pause processing
   * of DI_RAW_RECORDS_CHUNK_READ topic when flow control conditions met.
   *
   * @param tenantId     tenant id
   * @param counterValue counter value from DB after chunk received event
   */
  void trackChunkReceivedEvent(String tenantId, Integer counterValue);

  /**
   * If chunks duplicate event comes - need to correct flow control internal state
   * to avoid calculation errors.
   *
   * @param tenantId     tenant id
   * @param counterValue counter value from DB after chunk duplicate event
   */
  void trackChunkDuplicateEvent(String tenantId, Integer counterValue);

  /**
   * Tracks each successful DI_COMPLETED, DI_ERROR events, this method can also resume processing
   * of DI_RAW_RECORDS_CHUNK_PARSED topic when flow control conditions met.
   *
   * @param tenantId     tenant id
   * @param counterValue counter value from DB after complete event
   */
  void trackRecordCompleteEvent(String tenantId, Integer counterValue);
}
