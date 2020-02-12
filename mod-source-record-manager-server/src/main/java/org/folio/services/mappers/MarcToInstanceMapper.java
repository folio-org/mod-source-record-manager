package org.folio.services.mappers;

import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.folio.rest.jaxrs.model.Instance;
import org.folio.services.mappers.processor.Processor;
import org.folio.services.mappers.processor.parameters.MappingParameters;
import org.folio.services.parsers.RecordFormat;

import java.util.regex.Pattern;

public class MarcToInstanceMapper implements RecordToInstanceMapper {

  private static final Pattern UUID_PATTERN = Pattern.compile("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$");

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
    instance.getIdentifiers().forEach(identifier -> {
      if (StringUtils.isNoneBlank(identifier.getIdentifierTypeId())) {
        identifier.setIdentifierTypeId(fixUUID(identifier.getIdentifierTypeId()));
      }
    });
    instance.getClassifications().forEach(classification -> {
      if (StringUtils.isNoneBlank(classification.getClassificationTypeId())) {
        classification.setClassificationTypeId(fixUUID(classification.getClassificationTypeId()));
      }
    });
    instance.getContributors().forEach(contributor -> {
      if (StringUtils.isNoneBlank(contributor.getContributorTypeId())) {
        contributor.setContributorTypeId(fixUUID(contributor.getContributorTypeId()));
      }
    });
    return instance;
  }

  private String fixUUID(String uuid) {
    try {
      if (StringUtils.isNoneBlank(uuid)) {
        if (!UUID_PATTERN.matcher(uuid).matches()) {
          String fixed = uuid.split(" ")[0];
          if (UUID_PATTERN.matcher(fixed).matches()) {
            return fixed;
          }
        }
      }
    } catch (Exception ignored) {
    }
    return uuid;
  }

}
