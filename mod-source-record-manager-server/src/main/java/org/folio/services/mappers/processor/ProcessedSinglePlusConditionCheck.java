package org.folio.services.mappers.processor;

public class ProcessedSinglePlusConditionCheck extends ProcessedSingleItem {

  private boolean conditionsMet;

  public ProcessedSinglePlusConditionCheck(String data, boolean doBreak, boolean conditionsMet) {
    super(data, doBreak);
    this.conditionsMet = conditionsMet;
  }

  public boolean isConditionsMet() {
    return conditionsMet;
  }

  public void setConditionsMet(boolean conditionsMet) {
    this.conditionsMet = conditionsMet;
  }
}
