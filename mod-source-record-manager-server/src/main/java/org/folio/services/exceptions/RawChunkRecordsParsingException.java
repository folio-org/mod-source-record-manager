package org.folio.services.exceptions;

import org.folio.rest.jaxrs.model.RawRecordsDto;

/**
 * This exception can occur during parsing chunk of initial raw records.
 * It used in error handlers to send DI_ERROR for each failed record from the chunk.
 */
public class RawChunkRecordsParsingException extends RuntimeException {
  private RawRecordsDto rawRecordsDto;

  public RawChunkRecordsParsingException(Throwable cause, RawRecordsDto rawRecordsDto) {
    super(cause);
    this.rawRecordsDto = rawRecordsDto;
  }

  public RawRecordsDto getRawRecordsDto() {
    return rawRecordsDto;
  }
}
