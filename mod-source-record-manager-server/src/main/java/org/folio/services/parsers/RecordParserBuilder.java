package org.folio.services.parsers;

import java.util.Collections;
import java.util.List;

/**
 * Builder for creating a parser object by type of the records
 */
public final class RecordParserBuilder {

  private static List<RecordParser> rawRecordParsers = Collections.singletonList(new RawMarcRecordParser());

  private RecordParserBuilder() {
  }

  /**
   * TODO implement a more elaborate way to define a RecordParser //NOSONAR
   *
   * Currently this method builds specific parser based on the record format
   * Implementation will be changed in scope of (@link https://issues.folio.org/browse/MODSOURMAN-112)
   *
   * @param format - raw record format
   * @param rawRecord - raw record itself
   * @return - RecordParser for chosen record format
   */
  public static RecordParser buildParser(RecordFormat format, String rawRecord) {
    if (rawRecord.startsWith("{")) {
      return new JsonRecordParser();
    }
    return rawRecordParsers.stream()
      .filter(parser -> parser.getParserFormat().equals(format))
      .findFirst()
      .orElseThrow(() -> new RecordParserNotFoundException(String.format("Raw Record Parser was not found for Record Format: %s", format.getFormat())));
  }
}
