package org.folio.services.parsers;

import org.folio.rest.jaxrs.model.RecordsMetadata;

/**
 * Common interface for Record Parser. Parsers for each format should implement it
 */
public interface RecordParser {

  /**
   * Method parses record and return String with json representation of record
   *
   * @param rawRecord - String with raw record
   * @return - Wrapper for parsed record in json format.
   * Can contains errors descriptions if parsing was failed
   */
  ParsedResult parseRecord(String rawRecord);

  /**
   * @return - format which RecordParser can parse
   */
  RecordsMetadata.ContentType getParserFormat();
}
