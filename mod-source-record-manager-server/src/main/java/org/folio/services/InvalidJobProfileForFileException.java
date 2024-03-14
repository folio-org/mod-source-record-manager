package org.folio.services;

public class InvalidJobProfileForFileException extends Exception {

  public InvalidJobProfileForFileException(String message) {
    super(message);
  }

  public InvalidJobProfileForFileException(String message, Throwable cause) {
    super(message, cause);
  }
}
