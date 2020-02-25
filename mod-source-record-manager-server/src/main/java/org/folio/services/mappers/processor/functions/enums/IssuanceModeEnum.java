package org.folio.services.mappers.processor.functions.enums;

/**
 * Enum for "Mode of issuance" with values
 */
public enum IssuanceModeEnum {
  SINGLE_UNIT("single unit", 'a','c','d','m'),
  SERIAL("serial", 'b', 's'),
  INTEGRATING_RESOURCE("integrating resource", 'i'),
  UNSPECIFIED("unspecified");

  private String issuanceModeType;
  private char[] symbols;

  IssuanceModeEnum(String issuanceModeType, char... symbols){
    this.issuanceModeType = issuanceModeType;
    this.symbols = symbols;
  }

  public String getValue(){
    return issuanceModeType;
  }

  public char[] getSymbols(){
    return symbols;
  }
}
