package org.folio.services.parsers;

/**
 * Common interface for Source Record Parser. Parsers for each format should implement it
 */
public interface SourceRecordParser {

  /**
   * Method parse Source record and return String with json representation of record
   *
   * @param sourceRecord - String with source record
   * @return - Wrapper for parsed record in json format.
   * Can contains errors descriptions if parsing was failed
   */
  ParsedResult parseRecord(String sourceRecord);

  /**
   * @return - format which RecordParser can parse
   */
  RecordFormat getParserFormat();
}
