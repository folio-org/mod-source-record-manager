package org.folio.services.mappers;

import io.vertx.core.json.JsonObject;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.services.mappers.processor.parameters.MappingParameters;
import org.folio.services.mappers.processor.Processor;
import org.folio.services.parsers.RecordFormat;

public class MarcToInstanceMapper implements RecordToInstanceMapper {

  @Override
  public Instance mapRecord(JsonObject parsedRecord, MappingParameters mappingParameters, JsonObject mappingRules) {
    return new Processor()
      .process(parsedRecord, mappingParameters, mappingRules)
      .withSource(getMapperFormat().getFormat());
  }

  @Override
  public RecordFormat getMapperFormat() {
    return RecordFormat.MARC;
  }

}
