package org.folio.services.mappers;

import io.vertx.core.json.JsonObject;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.services.mappers.processor.parameters.MappingParameters;
import org.folio.services.parsers.RecordFormat;

/**
 * Common interface for Record to Instance mapper. Mappers for each format of Parsed Record should implement it
 */
public interface RecordToInstanceMapper {

  /**
   * Maps Parsed Record to Instance Record
   *
   * @param parsedRecord - JsonObject containing Parsed Record
   * @param mappingParameters - parameters needed for mapping functions
   * @return - Wrapper for parsed record in json format.
   * Can contains errors descriptions if parsing was failed
   */
  Instance mapRecord(JsonObject parsedRecord, MappingParameters mappingParameters);

  /**
   * Provides access to the MapperFormat
   *
   * @return - format which RecordMapper can map
   */
  RecordFormat getMapperFormat();
}
