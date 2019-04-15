package org.folio.services.mappers;

import org.folio.services.parsers.RecordFormat;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Builder for creating a mapper object by type of the records
 */
public final class RecordToInstanceMapperBuilder {

  private static List<RecordToInstanceMapper> availableMappers = Collections.singletonList(new MarcToInstanceMapper());

  private RecordToInstanceMapperBuilder() {
  }

  /**
   * @param format - record format
   * @return - RecordToInstanceMapper for the specified record format
   */
  public static RecordToInstanceMapper buildMapper(RecordFormat format) {
    Optional<RecordToInstanceMapper> recordToInstanceMapper = availableMappers.stream()
      .filter(mapper -> mapper.getMapperFormat().equals(format))
      .findFirst();
    return recordToInstanceMapper
      .orElseThrow(() -> new RecordToInstanceMapperNotFoundException("Record to Instance Mapper was not found for Record Format: " + format.getFormat()));
  }
}
