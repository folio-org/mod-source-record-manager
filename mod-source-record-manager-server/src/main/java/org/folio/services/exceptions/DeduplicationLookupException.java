package org.folio.services.exceptions;

/**
 * Thrown when the deduplication lookup for an incoming event fails because of a database error
 * (not because the event is a duplicate). The event has not been processed yet, so it is safe to redeliver it.
 */
public class DeduplicationLookupException extends RuntimeException {

  public DeduplicationLookupException(String message, Throwable cause) {
    super(message, cause);
  }
}
