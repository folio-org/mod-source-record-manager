package org.folio.services.parsers;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

/**
 * Builder for creating a parser object by type of the records
 */
public final class SourceRecordParserBuilder {

  private static List<SourceRecordParser> availableParsers = Arrays.asList(new MarcRecordParser());

  private SourceRecordParserBuilder() {
  }

  /**
   * @param format - source record format
   * @return - SourceRecordParser for chosen record format
   */
  public static SourceRecordParser buildParser(RecordFormat format) {
    Optional<SourceRecordParser> sourceRecordParser = availableParsers.stream().filter(parser -> parser.getParserFormat().equals(format)).findFirst();
    return sourceRecordParser.orElse(null);
  }
}
