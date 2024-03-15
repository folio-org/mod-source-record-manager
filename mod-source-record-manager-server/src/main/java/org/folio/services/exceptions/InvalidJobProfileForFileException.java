package org.folio.services.exceptions;

/**
 * This exception is thrown when an invalid job profile is selected for an uploaded file.
 */
public class InvalidJobProfileForFileException extends Exception {

  public InvalidJobProfileForFileException(String message) {
    super(message);
  }

  public InvalidJobProfileForFileException(String message, Throwable cause) {
    super(message, cause);
  }
}
