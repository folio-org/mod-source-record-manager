package org.folio.services.flowcontrol;

/**
 * Service to implement flow control logic to be able to import OCLC files in between imports of huge files.
 * Its necessary to not wait for importing 1 record file until other big files in progress.
 * Its can be suitable not only for OCLC, but also for other files that skipping DI_RAW_RECORDS_CHUNK_READ stage
 * and directly pushing messages to DI_RAW_RECORDS_CHUNK_PARSED topic.
 */
public interface FlowControlService {

  /**
   * Tracks each successful DI_RAW_RECORDS_CHUNK_PARSED event, this method can also pause processing
   * of DI_RAW_RECORDS_CHUNK_READ topic when flow control conditions met.
   *
   * @param tenantId the tenant id
   * @param initialRecordsSize initial records size in the batch
   */
  void trackChunkProcessedEvent(String tenantId, Integer initialRecordsSize);

  /**
   * Tracks each successful DI_COMPLETED, DI_ERROR events, this method can also resume processing
   * of DI_RAW_RECORDS_CHUNK_PARSED topic when flow control conditions met.
   *
   * @param tenantId the tenant id
   */
  void trackRecordCompleteEvent(String tenantId);
}
