package org.folio.services.mappers.processor.functions.enums;

public enum ElectronicAccessRelationshipEnum {
  RESOURCE('0', "resource"),
  VERSION_OF_RESOURCE('1', "version of resource"),
  RELATED_RESOURCE('2', "related resource"),
  NO_INFORMATION_PROVIDED('3', "no information provided");

  ElectronicAccessRelationshipEnum(char indicator2value, String name) {
    this.indicator2value = indicator2value;
    this.name = name;
  }

  private char indicator2value;
  private String name;

  public static String getNameByIndicator(char indicatorValue) {
    for (ElectronicAccessRelationshipEnum enumValue : values()) {
      if (indicatorValue == enumValue.indicator2value) {
        return enumValue.name;
      }
    }
    return NO_INFORMATION_PROVIDED.name;
  }
}
