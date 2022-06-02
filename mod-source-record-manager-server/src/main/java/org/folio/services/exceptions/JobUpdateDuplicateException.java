package org.folio.services.exceptions;

/**
 * Exception for errors while updating job.
 */
public class JobUpdateDuplicateException extends RuntimeException {
  public JobUpdateDuplicateException(String message) {
    super(message);
  }
}
