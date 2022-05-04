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
   * @param initialRecordsCount records count in the chunk
   */
  void trackChunkReceivedEvent(Integer initialRecordsCount);

  /**
   * If chunks duplicate event comes - need to correct flow control internal state
   * to avoid calculation errors.
   *
   * @param duplicatedRecordsCount records count in the chunk
   */
  void trackChunkDuplicateEvent(Integer duplicatedRecordsCount);

  /**
   * Tracks each successful DI_COMPLETED, DI_ERROR events, this method can also resume processing
   * of DI_RAW_RECORDS_CHUNK_PARSED topic when flow control conditions met.
   *
   */
  void trackRecordCompleteEvent();
}
