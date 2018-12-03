package org.folio.services.converters;

public enum Status {

  NEW("INITIALIZATION"),
  IMPORT_IN_PROGRESS("INITIALIZATION"),
  IMPORT_FINISHED("INITIALIZATION"),
  PARSING_IN_PROGRESS("PREPARING_FOR_PREVIEW"),
  PARSING_FINISHED("PREPARING_FOR_PREVIEW"),
  PROCESSING_IN_PROGRESS("PREPARING_FOR_PREVIEW"),
  PROCESSING_FINISHED("READY_FOR_PREVIEW"),
  COMMIT_IN_PROGRESS("RUNNING"),
  COMMITTED("RUNNING_COMPLETE"),
  ERROR("ERROR");

  private String uiStatus;

  Status(String uiStatus){
    this.uiStatus = uiStatus;
  }
  public String getUiStatus() {
    return uiStatus;
  }

}
