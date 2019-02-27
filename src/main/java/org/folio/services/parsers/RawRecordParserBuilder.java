package org.folio.services.parsers;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Builder for creating a parser object by type of the records
 */
public final class RawRecordParserBuilder {

  private static List<RawRecordParser> availableParsers = Collections.singletonList(new MarcRecordParser());

  private RawRecordParserBuilder() {
  }

  /**
   * @param format - raw record format
   * @return - RawRecordParser for chosen record format
   */
  public static RawRecordParser buildParser(RecordFormat format) {
    Optional<RawRecordParser> rawRecordParser = availableParsers.stream()
      .filter(parser -> parser.getParserFormat().equals(format))
      .findFirst();
    return rawRecordParser
      .orElseThrow(() -> new RawRecordParserNotFoundException("Raw Record Parser was not founded for Record Format: " + format.getFormat()));
  }
}
