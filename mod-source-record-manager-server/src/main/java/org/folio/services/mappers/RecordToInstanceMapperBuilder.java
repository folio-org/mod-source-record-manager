package org.folio.services.mappers;

import org.folio.services.parsers.RecordFormat;

import java.util.Collections;
import java.util.List;

/**
 * Builder for creating a mapper object by type of the records
 */
public final class RecordToInstanceMapperBuilder {

  private static List<RecordToInstanceMapper> mappers = Collections.singletonList(new MarcToInstanceMapper());

  private RecordToInstanceMapperBuilder() {
  }

  /**
   * Builds specific mapper based on the record format
   *
   * @param format - record format
   * @return - RecordToInstanceMapper for the specified record format
   */
  public static RecordToInstanceMapper buildMapper(RecordFormat format) {
    return  mappers.stream()
      .filter(mapper -> mapper.getMapperFormat().equals(format))
      .findFirst()
      .orElseThrow(() -> new RecordToInstanceMapperNotFoundException(String.format("Record to Instance Mapper was not found for Record Format: %s", format.getFormat())));
  }
}
