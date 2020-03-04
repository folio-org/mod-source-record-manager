package org.folio.services.journal;

/**
 * Custom exception for describing journal record mapping-processing error
 */
public class JournalRecordMapperException extends Exception {

  public JournalRecordMapperException(String message) {
    super(message);
  }
}
