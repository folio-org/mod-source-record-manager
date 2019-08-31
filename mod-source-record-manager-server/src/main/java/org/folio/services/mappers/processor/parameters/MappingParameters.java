package org.folio.services.mappers.processor.parameters;

import org.apache.commons.collections4.list.UnmodifiableList;
import org.folio.rest.jaxrs.model.ClassificationType;
import org.folio.rest.jaxrs.model.IdentifierType;

import java.util.List;

/**
 * Class to store parameters needed for mapping functions
 */
public class MappingParameters {

  private UnmodifiableList<IdentifierType> identifierTypes;
  private UnmodifiableList<ClassificationType> classificationTypes;

  public List<IdentifierType> getIdentifierTypes() {
    return identifierTypes;
  }

  public MappingParameters withIdentifierTypes(List<IdentifierType> identifierTypes) {
    this.identifierTypes = new UnmodifiableList<>(identifierTypes);
    return this;
  }

  public List<ClassificationType> getClassificationTypes() {
    return classificationTypes;
  }

  public MappingParameters withClassificationTypes(List<ClassificationType> classificationTypes) {
    this.classificationTypes = new UnmodifiableList<>(classificationTypes);
    return this;
  }
}
