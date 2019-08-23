package org.folio.services.mappers.processor;

import org.marc4j.marc.DataField;

public class RuleExecutionContext {

  private DataField dataField;
  private String data;

  public DataField getDataField() {
    return dataField;
  }

  public void setDataField(DataField dataField) {
    this.dataField = dataField;
  }

  public String getData() {
    return data;
  }

  public void setData(String data) {
    this.data = data;
  }
}
