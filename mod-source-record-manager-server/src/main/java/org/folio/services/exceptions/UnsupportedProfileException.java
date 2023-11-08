package org.folio.services.exceptions;

/**
 * This exception can occur when the DI job uses an unsupported job profile.
 */
public class UnsupportedProfileException extends RuntimeException {
  public UnsupportedProfileException(String message) {
    super(message);
  }
}
