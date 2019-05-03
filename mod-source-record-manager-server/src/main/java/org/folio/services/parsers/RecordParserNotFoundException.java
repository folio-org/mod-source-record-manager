package org.folio.services.parsers;

public class RecordParserNotFoundException extends RuntimeException {
  public RecordParserNotFoundException(String message) {
    super(message);
  }
}
