package org.folio.services.exceptions;

import org.folio.rest.jaxrs.model.Record;

import java.util.List;

/**
 * This exception is thrown when an incompatible job profile is selected for an uploaded file.
 * The exception contains records from an incoming chunk of file.
 */
public class InvalidJobProfileForFileException extends Exception {

  private final List<Record> records;

  public InvalidJobProfileForFileException(List<Record> records, String message) {
    super(message);
    this.records = records;
  }

  public List<Record> getRecords() {
    return records;
  }

}
