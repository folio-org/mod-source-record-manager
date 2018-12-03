package org.folio.services.converters;

import org.folio.rest.jaxrs.model.JobExecution;

public enum Status {

  NEW(JobExecution.UiStatus.INITIALIZATION.value()),
  IMPORT_IN_PROGRESS(JobExecution.UiStatus.INITIALIZATION.value()),
  IMPORT_FINISHED(JobExecution.UiStatus.INITIALIZATION.value()),
  PARSING_IN_PROGRESS(JobExecution.UiStatus.PREPARING_FOR_PREVIEW.value()),
  PARSING_FINISHED(JobExecution.UiStatus.PREPARING_FOR_PREVIEW.value()),
  PROCESSING_IN_PROGRESS(JobExecution.UiStatus.PREPARING_FOR_PREVIEW.value()),
  PROCESSING_FINISHED(JobExecution.UiStatus.READY_FOR_PREVIEW.value()),
  COMMIT_IN_PROGRESS(JobExecution.UiStatus.RUNNING.value()),
  COMMITTED(JobExecution.UiStatus.RUNNING_COMPLETE.value()),
  ERROR(JobExecution.UiStatus.ERROR.value());

  private String uiStatus;

  Status(String uiStatus) {
    this.uiStatus = uiStatus;
  }

  public String getUiStatus() {
    return uiStatus;
  }

}
