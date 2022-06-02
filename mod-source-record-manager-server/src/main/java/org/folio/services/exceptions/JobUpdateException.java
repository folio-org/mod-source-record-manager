package org.folio.services.exceptions;

/**
 * Exception for errors while updating job.
 */
public class JobUpdateException extends RuntimeException {
  public JobUpdateException(String message) {
    super(message);
  }
}
