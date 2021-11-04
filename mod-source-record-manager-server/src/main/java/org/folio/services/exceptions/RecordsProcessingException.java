package org.folio.services.exceptions;

import org.folio.rest.jaxrs.model.Record;

import java.util.List;

/**
 * Exception that accumulates records that processed with errors. This exception used in error handlers to
 * send DI_ERROR for each failed record.
 */
public class RecordsProcessingException extends RuntimeException {
  private List<Record> failedRecords;

  public RecordsProcessingException(String message, List<Record> failedRecords) {
    super(message);
    this.failedRecords = failedRecords;
  }

  public List<Record> getFailedRecords() {
    return failedRecords;
  }
}
