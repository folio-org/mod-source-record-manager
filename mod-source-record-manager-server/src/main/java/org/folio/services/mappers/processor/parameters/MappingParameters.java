package org.folio.services.mappers.processor.parameters;

import org.apache.commons.collections4.list.UnmodifiableList;
import org.folio.rest.jaxrs.model.ClassificationType;
import org.folio.rest.jaxrs.model.ContributorNameType;
import org.folio.rest.jaxrs.model.ElectronicAccessRelationship;
import org.folio.rest.jaxrs.model.IdentifierType;
import org.folio.rest.jaxrs.model.InstanceFormat;
import org.folio.rest.jaxrs.model.InstanceType;

import java.util.List;

/**
 * Class to store parameters needed for mapping functions
 */
public class MappingParameters {

  private UnmodifiableList<IdentifierType> identifierTypes;
  private UnmodifiableList<ClassificationType> classificationTypes;
  private UnmodifiableList<InstanceType> instanceTypes;
  private UnmodifiableList<ElectronicAccessRelationship> electronicAccessRelationship;
  private UnmodifiableList<InstanceFormat> instanceFormats;
  private UnmodifiableList<ContributorNameType> contributorNameTypes;

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

  public List<InstanceType> getInstanceTypes() {
    return instanceTypes;
  }

  public MappingParameters withInstanceTypes(List<InstanceType> instanceTypes) {
    this.instanceTypes = new UnmodifiableList<>(instanceTypes);
    return this;
  }

  public List<ElectronicAccessRelationship> getElectronicAccessRelationships() {
    return electronicAccessRelationship;
  }

  public MappingParameters withElectronicAccessRelationships(List<ElectronicAccessRelationship> electronicAccessRelationship) {
    this.electronicAccessRelationship = new UnmodifiableList<>(electronicAccessRelationship);
    return this;
  }

  public List<InstanceFormat> getInstanceFormats() {
    return instanceFormats;
  }

  public MappingParameters withInstanceFormats(List<InstanceFormat> instanceFormats) {
    this.instanceFormats = new UnmodifiableList<>(instanceFormats);
    return this;
  }

  public List<ContributorNameType> getContributorNameTypes() {
    return contributorNameTypes;
  }

  public MappingParameters withContributorNameTypes(List<ContributorNameType> contributorNameTypes) {
    this.contributorNameTypes = new UnmodifiableList<>(contributorNameTypes);
    return this;
  }
}
