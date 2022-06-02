package org.folio.services.exceptions;

/**
 * Exception for errors while updating job.
 */
public class JobDuplicateUpdateException extends RuntimeException {
  public JobDuplicateUpdateException(String message) {
    super(message);
  }
}
