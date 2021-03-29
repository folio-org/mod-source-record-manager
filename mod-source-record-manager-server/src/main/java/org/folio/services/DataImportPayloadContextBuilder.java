package org.folio.services;

import io.vertx.core.json.JsonObject;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.Record;

import java.util.HashMap;

interface DataImportPayloadContextBuilder {

  HashMap<String, String> buildFrom(Record record, JsonObject mappingRules, MappingParameters mappingParameters);

}