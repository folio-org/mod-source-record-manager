package org.folio.services.mappers.processor.functions.enums;

/**
 * Enum for "Mode of issuance" with values
 */
public enum IssuanceModeEnum {
  SINGLE_UNIT("single unit"),
  SERIAL("serial"),
  INTEGRATING_RESOURCE("integrating resource"),
  UNSPECIFIED("unspecified");

  private String issuanceModeType;

  IssuanceModeEnum(String issuanceModeType){
    this.issuanceModeType = issuanceModeType;
  }

  public String getValue(){
    return issuanceModeType;
  }
}
