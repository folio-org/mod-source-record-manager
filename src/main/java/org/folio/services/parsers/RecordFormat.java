package org.folio.services.parsers;

/**
 * Source Record formats
 */
public enum RecordFormat {
  MARC("marc");

  private String format;

  RecordFormat(String format) {
    this.format = format;
  }

  public String getFormat() {
    return format;
  }
}
