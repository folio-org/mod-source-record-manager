package org.folio.services.mappers;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.Classification;
import org.folio.rest.jaxrs.model.Identifier;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.services.mappers.processor.Processor;
import org.folio.services.mappers.processor.parameters.MappingParameters;
import org.folio.services.parsers.RecordFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class MarcToInstanceMapper implements RecordToInstanceMapper {

  private static final Pattern UUID_DUPLICATE_PATTERN = Pattern.compile("([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12} ){2,}");
  private static final String BLANK_STRING = " ";
  private static final Logger LOGGER = LoggerFactory.getLogger(MarcToInstanceMapper.class);

  @Override
  public Instance mapRecord(JsonObject parsedRecord, MappingParameters mappingParameters, JsonObject mappingRules) {
    Instance instance = new Processor().process(parsedRecord, mappingParameters, mappingRules);
    if (instance != null) {
      instance = fixDuplicatedUUIDs(instance.withSource(getMapperFormat().getFormat()));
    }
    return instance;
  }

  @Override
  public RecordFormat getMapperFormat() {
    return RecordFormat.MARC;
  }

  private Instance fixDuplicatedUUIDs(Instance instance) {
    List<Identifier> splitIdentifiers = new ArrayList<>();
    List<Classification> splitClassification = new ArrayList<>();
    instance.getIdentifiers().forEach(identifier -> {
      if (StringUtils.isNoneBlank(identifier.getIdentifierTypeId())
        && UUID_DUPLICATE_PATTERN.matcher(identifier.getIdentifierTypeId() + BLANK_STRING).matches()) {
        String[] uuids = identifier.getIdentifierTypeId().split(BLANK_STRING);
        String[] values = identifier.getValue().split(BLANK_STRING);
        if (uuids.length > 1 && values.length > 1) {
          identifier.setIdentifierTypeId(uuids[0]);
          identifier.setValue(values[0]);
        }
        for (int i = 1; i < uuids.length; i++) {
          Identifier newIdentifier = new Identifier().withIdentifierTypeId(uuids[i]);
          if (values.length > i) {
            newIdentifier.setValue(i == uuids.length - 1
              ? String.join(BLANK_STRING, Arrays.copyOfRange(values, i, values.length))
              : values[i]);
            splitIdentifiers.add(newIdentifier);
          }
        }
      }
    });
    instance.getClassifications().forEach(classification -> {
      if (StringUtils.isNoneBlank(classification.getClassificationTypeId())
        && UUID_DUPLICATE_PATTERN.matcher(classification.getClassificationTypeId() + BLANK_STRING).matches()) {
        String[] uuids = classification.getClassificationTypeId().split(BLANK_STRING);
        String[] values = classification.getClassificationNumber().split(BLANK_STRING);
        if (uuids.length > 1 && values.length > 1) {
          classification.setClassificationTypeId(uuids[0]);
          classification.setClassificationNumber(values[0]);
        }
        for (int i = 1; i < uuids.length; i++) {
          Classification newClassification = new Classification().withClassificationTypeId(uuids[i]);
          if (values.length > i) {
            newClassification.setClassificationNumber(i == uuids.length - 1
              ? String.join(BLANK_STRING, Arrays.copyOfRange(values, i, values.length))
              : values[i]);
            splitClassification.add(newClassification);
          }
        }
      }
    });
    instance.getIdentifiers().addAll(splitIdentifiers);
    instance.getClassifications().addAll(splitClassification);
    return instance;
  }
}