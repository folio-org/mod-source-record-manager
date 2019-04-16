package org.folio.services.parsers;

import java.util.Collections;
import java.util.List;

/**
 * Builder for creating a parser object by type of the records
 */
public final class RawRecordParserBuilder {

  private static List<RawRecordParser> parsers = Collections.singletonList(new MarcRecordParser());

  private RawRecordParserBuilder() {
  }

  /**
   * Builds specific parser based on the record format
   *
   * @param format - raw record format
   * @return - RawRecordParser for chosen record format
   */
  public static RawRecordParser buildParser(RecordFormat format) {
    return parsers.stream()
      .filter(parser -> parser.getParserFormat().equals(format))
      .findFirst()
      .orElseThrow(() -> new RawRecordParserNotFoundException(String.format("Raw Record Parser was not founded for Record Format: %s", format.getFormat())));
  }
}
