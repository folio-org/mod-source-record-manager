package org.folio.services.parsers;

import org.folio.rest.jaxrs.model.RecordsMetadata;

import java.util.Arrays;
import java.util.List;

/**
 * Builder for creating a parser object by type of the records
 */
public final class RecordParserBuilder {

  private static List<RecordParser> rawRecordParsers = Arrays.asList(new MarcRecordParser(), new JsonRecordParser(), new XmlRecordParser(), new EdifactRecordParser());

  private RecordParserBuilder() {
  }

  /**
   * Builds specific parser based on the record content type
   *
   * @param recordContentType - record content type
   * @return - RecordParser for chosen record content type
   */
  public static RecordParser buildParser(RecordsMetadata.ContentType recordContentType) {
    return rawRecordParsers.stream()
      .filter(parser -> parser.getParserFormat().equals(recordContentType))
      .findFirst()
      .orElseThrow(() -> new RecordParserNotFoundException(String.format("Raw Record Parser was not found for record content type: %s", recordContentType)));
  }
}
